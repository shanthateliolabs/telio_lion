is_whiskers = False


class Threshold:

    def __init__(self):
        with open('threshold.config', 'r') as f:
            self.threshold = float(f.read())
        print('Threshold Init Done')

    def get_threshold(self):
        return self.threshold

    def set_threshold(self, sign_str, delta_str):
        try:
            delta = float(delta_str)
        except Exception as e:
            print(str(e))
            delta = 0.0
        if sign_str == '+':
            self.threshold = round(self.threshold + delta, 2)
        else:
            self.threshold = round(self.threshold - delta, 2)
        if self.threshold < 0.10:
            self.threshold = 0.10
        with open('threshold.config', 'w') as f:
            f.write(str(self.threshold))


threshold = Threshold()
