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
        zeroshot_topic_list = run.data.zs_topic_list
        cleaned_df = run.data.cleaned_df
        # TODO: Leverage gpu acceleration to improve sample size
        sampled_df = cleaned_df.sample(100000)
        sentence_model = SentenceTransformer("all-MiniLM-L6-v2")
        embeddings = sentence_model.encode(sampled_df["cleaned_description"] \
                                           .tolist(), show_progress_bar=False)

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
                ["cleaned_description"].tolist())
        print(topic_model.get_topic_info().head(10))
        self.topic_info_df = topic_model.get_topic_info().head(30)

        # Append predictions to fit set
        sampled_df["predicted_topic"] = topics
        sampled_df["predicted_merchant"] = sampled_df["predicted_topic"] \
        .apply(lambda _:topic_model.get_topic_info(_)["Name"])

        self.fit_prediction_df = sampled_df
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
