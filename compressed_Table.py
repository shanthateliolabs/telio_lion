import base64
from configparser import ConfigParser
from os import read
import cv2
import imagehash
from numpy.core.fromnumeric import compress
import pandas as pd
import PIL
import psycopg2
import matplotlib.pyplot as plt
import scipy.fftpack
import hashlib
from db_driver import aggregate ,get_current_count
# handle = "localhost"
handle = "34.93.181.52"
database = "telio_lions"



def get_all_compressed_faces(page_number,limit):
    limit = limit
    page_number = page_number - 1
    offsets = limit * page_number
    #print(offsets,limit)
    ret = 0
    conn = None
    rv = dict()
    sql = "SELECT comp_img.id,comp_img.name,comp_img.face, ln_data.sex, ln_data.status FROM compressed_images comp_img "\
         "INNER JOIN lion_data ln_data "\
        "ON comp_img.id = ln_data.id offset %s limit %s;"
    try :
        conn = psycopg2.connect(host=handle,
                                database=database,
                                user="postgres",
                                password="admin")

        cur = conn.cursor()
        cur.execute(sql,(offsets,limit,))
        records = cur.fetchall()
        lion_instance = list()
        for record in records:
            one_record = dict()
            one_record['id'] = record[0]
            one_record['name'] = record[1]
            one_record['face'] = record[2]
            one_record['gender']=record[3]
            one_record['status']=record[4]
            lion_instance.append(one_record)
        #print(len(lion_instance))
        rv['lion'] = lion_instance
        rv['total_count'] = get_current_count()

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        rv = dict()
        ret = -1
    finally:
        if conn is not None:
            conn.close()
        return rv, ret




    # except (Exception, psycopg2.DatabaseError) as error:
    #     print("DB Error: " + str(error))
    #     rv = dict()
    #     ret = -1
    # finally:
    #     if conn is not None:
    #         conn.close()
    #     return rv, ret


def get_all_compressed_lions():
    ret = 0
    conn = None
    rv = dict()
    sql = "SELECT C1.name,C1.microchip_number, L1.sex, L1.status, L1.click_date, L1.upload_date, L1.latitude, L1.longitude, C1.face FROM compressed_images C1 "\
         "INNER JOIN lion_data L1 "\
        "ON C1.id = L1.id;"

    try:
        conn = psycopg2.connect(host=handle,
                                database=database,
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        cur.close()
        df = pd.DataFrame(records, columns=['name','microchip_number', 'sex', 'status', 'click_date',
                                            'upload_date', 'latitude', 'longitude', 'face'])
        df = df.groupby(['name'])['sex', 'status', 'click_date', 'upload_date', 'latitude', 'longitude', 'face'].apply(lambda x: aggregate(x)).reset_index()
        lions = list()
        for index, row in df.iterrows():
            info = dict()
            info['name'] = row['name']
            info['sex'] = row['sex']
            info['status'] = row['status']
            info['click_date'] = str(row['click_date'])
            info['upload_date'] = str(row['upload_date'])
            info['latitude'] = row['latitude']
            info['longitude'] = row['longitude']
            info['face'] = row['face']
            lions.append(info)
        rv['lions'] = lions
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        rv = dict()
        ret = -1
    finally:
        if conn is not None:
            conn.close()
        return rv, ret


# string_return
def get_base64_str(image):
    try:
        with open(image, "rb") as imageFile:
            base64_str = str(base64.b64encode(imageFile.read()))[2:-1]
        return base64_str
    except Exception as e:
        return ''

def duplicate_img_detected(hash_value):
    ret = False
    status = "Success"
    conn = None
    sql = "SELECT hash_value FROM lion_data ;"
    try:
        conn = psycopg2.connect(host= handle,
                                database=database,
                                user="postgres",
                                password="admin")

        cur = conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        cur.close()
        df = pd.DataFrame(records,columns=['hash'])
        hash_list = df['hash'].tolist()

        if hash_value not in hash_list:
            ret = 0
        else:
            ret = 1
            status = "Duplicate image Detected"

    except (Exception, psycopg2.DatabaseError) as error:
            print("DB Error: " + str(error))
            ret_str = str(error)
            ret = -1
            status = "DB error"
    finally:
        if conn is not None:
            conn.close()
        return ret ,status


def img_hash_value(images):
    try:
        hash_value = imagehash.dhash(PIL.Image.open(images))
        print('Hash_value ',str(hash_value))
        return hash_value
    except Exception as e:
        return ''


# def verify():
#     def init():
#         if not if_table_exists(table_name='compressed_TB'):
#             create_user_data_table()
#         else:
#             insert_compressed_data()

def match_lion(face_embedding, whisker_embedding, ret):
    ret['threshold'] = threshold.get_threshold()
    match_data = list()
    embeddings = get_all_lion_embeddings()
    if len(face_embedding) == 0:
        face_emb = [float(0.0) for _ in range(0, embedding_dim, 1)]
    else:
        face_emb = list()
        for x in face_embedding.split(','):
            try:
                face_emb.append(float(x))
            except Exception as e:
                print(x)
                face_emb.append(float('0.0'))
    if len(whisker_embedding) == 0:
        whisker_emb = [float(0.0) for _ in range(0, embedding_dim, 1)]
    else:
        whisker_emb = list()
        for x in whisker_embedding.split(','):
            try:
                whisker_emb.append(float(x))
            except Exception as e:
                print(x)
                whisker_emb.append(float('0.0'))
    no_face_or_whisker = 1
    for embedding in embeddings:
        ref_id = embedding[0]
        ref_lion_name = embedding[1]
        ref_face_embedding = list()
        for x in embedding[2].split(','):
            try:
                ref_face_embedding.append(float(x))
            except Exception as e:
                print(x)
                ref_face_embedding.append(float('0.0'))
        ref_whisker_embedding = list()
        for x in embedding[3].split(','):
            try:
                ref_whisker_embedding.append(float(x))
            except Exception as e:
                print(x)
                ref_whisker_embedding.append(float('0.0'))
        face_distance = spatial.distance.cosine(ref_face_embedding, face_emb)
        whisker_distance = spatial.distance.cosine(ref_whisker_embedding, whisker_emb)
        if is_whiskers:
            d = face_distance
        else:
            d = whisker_distance
        if d != 0:
            no_face_or_whisker = 0
            match_data.append((ref_id, ref_lion_name, face_distance, whisker_distance))

    if is_whiskers:
        index = 3
    else:
        index = 2

    if len(match_data) == 0:
        if no_face_or_whisker:
            ret['type'] = 'Unknown'
            ret['distance'] = 1.00
        else:
            ret['type'] = 'New'
            ret['distance'] = threshold.get_threshold()
    else:
        match_data.sort(key=lambda x1: x1[index])

    print(match_data)

    if len(match_data) > 0:
        _1st_match = match_data[0]
        d_1st = _1st_match[index]
        if d_1st <= threshold.get_threshold():
            ret['type'] = 'Similar'
            ret['similar'] = [{'id': _1st_match[0], 'name': _1st_match[1]}]
            ret['distance'] = round(d_1st, 2)
        else:
            ret['type'] = 'New'
            ret['distance'] = round(d_1st, 2)
    if len(match_data) > 1:
        _2nd_match = match_data[1]
        d_2nd = _2nd_match[index]
        if d_2nd <= threshold.get_threshold():
            ret['similar'].append({'id': _2nd_match[0], 'name': _2nd_match[1]})
            ret['distance'] = round(d_2nd, 2)
    if len(match_data) > 2:
        _3rd_match = match_data[2]
        d_3rd = _3rd_match[index]
        if d_3rd <= threshold.get_threshold():
            ret['similar'].append({'id': _3rd_match[0], 'name': _3rd_match[1]})
            ret['distance'] = round(d_3rd, 2)
    return ret

def insert_compressed_data(_id,microchip_number, name,
                           image, face,
                           whisker, lear,
                           rear, leye,
                           reye, nose
                           ):
    ret = 0
    status = "Success"
    conn = None
    try:
        try:
            image_bytes = get_base64_str(image)
        except Exception as e:
            image_bytes = ''
            pass
        face_bytes = get_base64_str(face)
        whisker_bytes = get_base64_str(whisker)
        try:
            lear_bytes = get_base64_str(lear)
        except Exception as e:
            lear_bytes = ''
            pass
        try:
            rear_bytes = get_base64_str(rear)
        except Exception as e:
            rear_bytes = ''
            pass
        try:
            leye_bytes = get_base64_str(leye)
        except Exception as e:
            leye_bytes = ''
            pass

        try:
            reye_bytes = get_base64_str(reye)
        except Exception as e:
            reye_bytes = ''
            pass
        try:
            nose_bytes = get_base64_str(nose)
        except Exception as e:
            nose_bytes = ''
            pass

        # try:
        #     hash_value = hash_value
        # except Exception as e:
        #     hash_value = ''
        #     pass

        sql = """INSERT INTO compressed_images VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING ID;"""
        conn = psycopg2.connect(host=handle,
                            database=database,
                            user="postgres",
                            password="admin")
        cur = conn.cursor()
        cur.execute(sql, (_id,microchip_number,
                      name,
                      image_bytes,
                      face_bytes,
                      whisker_bytes,
                      lear_bytes,
                      rear_bytes,
                      leye_bytes,
                      reye_bytes,
                      nose_bytes,
                      ))
        _id = cur.fetchone()[0]
        if _id:
            conn.commit()
            print("Committed --> " + str(_id))
        else:
            ret = -1
            status = "Failed to insert data."

    except (Exception, psycopg2.DatabaseError) as error:

        print("DB Error: " + str(error))
        ret = -1
        status = str(error)

    finally:
        if conn is not None:
            conn.close()





def create_compressed_table():
    ret = 0
    status = "Success"
    conn = None

    sql = sql = "CREATE TABLE compressed_images ( id text PRIMARY KEY," \
                "microchip_number text,"\
                "name text, " \
                "image text, " \
                "face text, " \
                "whisker text, " \
                "l_ear text, " \
                "r_ear text, " \
                "l_eye text, " \
                "r_eye text, " \
                "nose text);"
    try:
        conn = psycopg2.connect(host=handle,
                                database=database,
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        status = str(error)
        ret = -1
    finally:
        if conn is not None:
            conn.close()
        return ret, status

