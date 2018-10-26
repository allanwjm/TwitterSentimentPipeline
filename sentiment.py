from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()


def get_sentiment_scores(s):
    return _analyzer.polarity_scores(s)
