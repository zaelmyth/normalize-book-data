import re
import unicodedata


class Normalizer:
    @staticmethod
    def normalize(name):
        name = name.lower()
        name = Normalizer.__separate_initials(name)
        name = Normalizer.__remove_punctuation(name)
        name = Normalizer.__standardize_spacing(name)
        name = Normalizer.__sort_words(name)
        name = Normalizer.__remove_diacritics(name)

        return name

    @staticmethod
    def __separate_initials(name):
        return re.sub(r'(?<=[a-zA-Z])(?=[A-Z])', ' ', name)

    @staticmethod
    def __remove_punctuation(name):
        return re.sub(r'[^\w\s\u0600-\u06FF\u0400-\u04FF\u4E00-\u9FFF]', ' ', name)

    @staticmethod
    def __standardize_spacing(name):
        return re.sub(r'\s+', ' ', name).strip()

    @staticmethod
    def __sort_words(name):
        words = name.split()
        words.sort()

        return ' '.join(words)

    @staticmethod
    def __remove_diacritics(name):
        return ''.join(c for c in unicodedata.normalize('NFKD', name) if not unicodedata.combining(c))
