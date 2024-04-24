from metaflow import Flow, FlowSpec, step


class MerchantModelFlow(FlowSpec):
    """
    A flow to clean the transaction descriptions

    This flow performs the following steps:
    1) Loads the cleaned data from the prior flow.
    2) Generates embeddings for the data to improve performance.
    3) Fits a BERTopic model to the data
    """

    @step
    def start(self):
        """
        The start step:
        1) Loads the data into a pandas dataframe
        2) Generates embeddings for the data to improve performance.
        3) Fits a BERTopic model to the data

        TODO: split up above into separate steps.

        """

        from bertopic import BERTopic
        from bertopic.representation import KeyBERTInspired
        import pandas as pd
        from sentence_transformers import SentenceTransformer

        run = Flow("DescriptionCleanFlow").latest_successful_run
        cleaned_df = run.data.cleaned_df
        # TODO: Leverage gpu acceleration to improve sample size
        sampled_df = cleaned_df.sample(100000)
        sampled_test_df = cleaned_df.sample(100000)
        sentence_model = SentenceTransformer("all-MiniLM-L6-v2")
        embedding_df = pd.concat([sampled_df, sampled_test_df])
        embeddings = sentence_model.encode(sampled_df["cleaned_description"] \
                                           .tolist(), show_progress_bar=False)

        # We define a number of merchants that we know are in the documents
        zeroshot_topic_list = ["square*","cashapp*","toast*","non-merchant",
                               "doordash*","target","walmart", "mcdonalds",
                               "apple", "uber", "google", "amazon", "shell",
                               "circlek", "7-eleven", "dollargeneral",
                               "dollartree", "lyft", "tacobell", "exxon",
                               "starbucks", "chevron", "wendys", "365market",
                               "wawa", "familydollar", "speedway"]


        # We fit our model using the zero-shot merchants
        # and we define a minimum similarity. For each document,
        # if the similarity does not exceed that value, it will be used
        # for clustering instead.
        topic_model = BERTopic(
        embedding_model="thenlper/gte-small",
        min_topic_size=200,
        zeroshot_topic_list=zeroshot_topic_list,
        zeroshot_min_similarity=.82,
        calculate_probabilities=False,
        representation_model=KeyBERTInspired())

        topics, _ = topic_model.fit_transform(sampled_df \
                                              ["cleaned_description"] \
                                              .tolist(), embeddings)

        test_topics, _ = topic_model.transform(sampled_test_df \
                                              ["cleaned_description"] \
                                              .tolist(), embeddings)

        # Append predictions to fit set
        sampled_df["predicted_topic"] = topics
        sampled_df["predicted_merchant"] = sampled_df["predicted_topic"] \
        .apply(lambda _:topic_model.get_topic_info(_)["Name"])

        #Append predictions to test set
        sampled_test_df["predicted_topic"] = test_topics
        sampled_test_df["predicted_merchant"] = sampled_test_df \
                ["predicted_topic"].apply(lambda _: \
                topic_model.get_topic_info(_)["Name"])

        self.fit_prediction_df = sampled_df
        self.transform_prediction_df = sampled_test_df
        self.topic_info_df = topic_model.get_topic_info()
        self.next(self.end)

    @step
    def end(self):
        """
        The end step:
        1) Prints acknowledgement of end.
        """
        print("MerchantModelFlow is complete")

if __name__ == "__main__":
    MerchantModelFlow()
