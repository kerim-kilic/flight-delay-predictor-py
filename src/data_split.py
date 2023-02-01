def split_data(data, ratio, seed):
    # Separate positives and negatives.
    positive_samples = data[data["delay"] == "1"]
    negative_samples = data[data["delay"] == "0"]

    positive_samples_split = positive_samples.randomSplit([ratio, (1-ratio)], seed=seed)
    negative_samples_split = negative_samples.randomSplit([ratio, (1-ratio)], seed=seed)

    negative_samples_undersampled = negative_samples_split[0].sample(positive_samples_split[0].count()/negative_samples_split[0].count(), seed=seed)

    train_data = positive_samples_split[0].union(negative_samples_undersampled)
    test_data = positive_samples_split[1].union(negative_samples_split[1])

    return train_data, test_data