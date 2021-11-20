from ..src import twitter_worker

import mock
import unittest


class TwitterWorkerTestCase(unittest.TestCase):

    def test_normalize_sentiment_value(self):
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(1), 10)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(0.5), 9)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(0.2), 7)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(0), 5)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(-0.2), 2)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(-0.5), 1)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_sentiment_value(-1), 0)

    def test_normalize_abstract_value(self):
        self.assertEqual(twitter_worker.TwitterWorker.normalize_abstract_value(0.95), 3)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_abstract_value(0.85), 5)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_abstract_value(0.55), 10)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_abstract_value(0.25), 3)
        self.assertEqual(twitter_worker.TwitterWorker.normalize_abstract_value(0), 1)

    def test_score_length(self):
        self.assertEqual(twitter_worker.score_length(40), 3)
        self.assertEqual(twitter_worker.score_length(70), 6)
        self.assertEqual(twitter_worker.score_length(120), 10)

    def test_score_type(self):
        self.assertEqual(twitter_worker.score_type('quoted'), 0.6)
        self.assertEqual(twitter_worker.score_type('replied_to'), 0.7)
        self.assertEqual(twitter_worker.score_type('retweeted'), 0.1)
        self.assertEqual(twitter_worker.score_type('tweet'), 1)

    def test_score_time(self):
        self.assertEqual(twitter_worker.score_time(1), 30)
        self.assertEqual(twitter_worker.score_time(7), 20)
        self.assertEqual(twitter_worker.score_time(30), 12.52130303491482)
        self.assertEqual(twitter_worker.score_time(365), 1)


if __name__ == '__main__':
    unittest.main()
