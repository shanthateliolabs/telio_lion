import pandas as pd
from keras import backend as k
from keras.applications import ResNet50
from keras.models import Model
from keras.layers import Input, Dense, Dropout, Lambda
from keras.optimizers import Adam
from sklearn.model_selection import train_test_split

from train_utils import image_size, path_csv, SampleGen, gen, embedding_dim


def triplet_loss(inputs, dist='sqeuclidean', margin='maxplus'):
    anchor, positive, negative = inputs
    positive_distance = k.square(anchor - positive)
    negative_distance = k.square(anchor - negative)
    if dist == 'euclidean':
        positive_distance = k.sqrt(k.sum(positive_distance, axis=-1, keepdims=True))
        negative_distance = k.sqrt(k.sum(negative_distance, axis=-1, keepdims=True))
    elif dist == 'sqeuclidean':
        positive_distance = k.sum(positive_distance, axis=-1, keepdims=True)
        negative_distance = k.sum(negative_distance, axis=-1, keepdims=True)
    loss = positive_distance - negative_distance
    if margin == 'maxplus':
        loss = k.maximum(0.0, 2 + loss)
    elif margin == 'softplus':
        loss = k.log(1 + k.exp(loss))
    return k.mean(loss)


def l2_norm(y):
    from keras.backend import l2_normalize
    r = l2_normalize(y, axis=1)
    return r


def get_model():
    base_model = ResNet50(weights='imagenet', include_top=False, pooling='max')
    for layer in base_model.layers:
        layer.trainable = False

    x = base_model.output
    x = Dropout(0.6)(x)
    x = Dense(embedding_dim)(x)
    x = Lambda(l2_norm)(x)
    _embedding_model = Model(base_model.input, x, name="embedding")

    input_shape = (image_size, image_size, 3)
    anchor_input = Input(input_shape, name='anchor_input')
    positive_input = Input(input_shape, name='positive_input')
    negative_input = Input(input_shape, name='negative_input')
    anchor_embedding = _embedding_model(anchor_input)
    positive_embedding = _embedding_model(positive_input)
    negative_embedding = _embedding_model(negative_input)

    inputs = [anchor_input, positive_input, negative_input]
    outputs = [anchor_embedding, positive_embedding, negative_embedding]

    _triplet_model = Model(inputs, outputs)
    from keras.backend import mean
    _triplet_model.add_loss(mean(triplet_loss(outputs)))

    return _embedding_model, _triplet_model


if __name__ == "__main__":
    embedding_model, triplet_model = get_model()
    triplet_model.compile(loss=None, optimizer=Adam(0.01), metrics=['accuracy'])

    data = pd.read_csv(path_csv)
    train, test = train_test_split(data, train_size=0.9, random_state=1337)
    file_id_mapping_train = {k: v for k, v in zip(train.Image.values, train.Id.values)}
    file_id_mapping_test = {k: v for k, v in zip(test.Image.values, test.Id.values)}
    gen_tr = gen(SampleGen(file_id_mapping_train))
    gen_te = gen(SampleGen(file_id_mapping_test))

    history = triplet_model.fit_generator(gen_tr,
                                          validation_data=gen_te,
                                          epochs=10,
                                          verbose=1,
                                          workers=1,
                                          steps_per_epoch=30,
                                          validation_steps=20,
                                          use_multiprocessing=False)
    embedding_model.save('facenet_keras.h5')
