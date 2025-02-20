from sentence_transformers import SentenceTransformer
from sklearn.base import BaseEstimator, TransformerMixin
import spacy
import re
import numpy as np

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

# TextCleaner class definition
class TextCleaner(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.boilerplate_phrases = [
            "contact", "for more info", "call", "viewings", "visit",
            "arrange", "available now", "great investment", "schedule",
            "agent", "short let", "long let", "terms", "income", "photo", "please note"
        ]

    def simple_text_clean(self, text):
        # Remove URLs
        url_pattern = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
        text = re.sub(url_pattern, "", text)

        # Add space after periods
        pattern = r"\.(\S)"
        text = re.sub(pattern, r". \1", text)

        # Tokenize the text into sentences using spaCy
        doc = nlp(text)

        # Filter out sentences containing boilerplate phrases
        cleaned_sentences = [
            sent.text for sent in doc.sents if not any(phrase in sent.text.lower() for phrase in self.boilerplate_phrases)
        ]

        # Join cleaned sentences
        cleaned_text = " ".join(cleaned_sentences)

        # If cleaned text is empty, use the original description
        if len(cleaned_text) == 0:
            return text  # Fall back to the original description
        return cleaned_text

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return [self.simple_text_clean(text) for text in X]

# EmbeddingGenerator class definition
class EmbeddingGenerator(BaseEstimator, TransformerMixin):
    def __init__(self, model_name='all-MiniLM-L6-v2', batch_size=64):
        self.model_name = model_name
        self.model = SentenceTransformer(self.model_name)
        self.batch_size = batch_size

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return self.model.encode(X, batch_size=self.batch_size, show_progress_bar=True)
    
    
    
class FeatureAdder(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # Add new features
        X["sin_angle"] = np.sin(X["angle_from_center"])
        X["cos_angle"] = np.cos(X["angle_from_center"])

        # Normalize latitude and longitude
        X["latitude"]  = (X["latitude"] - 51.5072) / 0.18
        X["longitude"] = (X["longitude"] + 0.1276) / 0.35

        return X




