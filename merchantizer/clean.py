from metaflow import Flow, FlowSpec, step
import re

def clean_text(text):
    """
    Make text lowercase,
    replace amazon variations (amzn) with amazon,
    replace wal-mart variations (wal mart/wm supercenter) with wal-mart,
    clean up the dollar store variations (dollar tree/dollar general/
    family dollar),
    add dashes between some brands to differentiate them from words,
    expand text for doordash/square/paypal to make them more expressive,
    remove backslashes
    """
    text = text.lower()
    text = re.sub(r"amzn\w*","amazon ",text)
    text = re.sub(r"wal mart","wal-mart ",text)
    text = re.sub(r"wm supercenter","wal-mart ",text)
    text = re.sub(r"wal","wal-mart ",text)
    text = re.sub(r"sq\w*","square* ",text)
    text = re.sub(r"cash app","cashapp* ",text)
    text = re.sub(r"dd\w*","doordash* ",text)
    text = re.sub(r"tst*","toast* ",text)
    text = re.sub(r".com\w*"," ",text)
    text = re.sub(r"qt\w*","non-merchant",text)
    text = re.sub(r"dollar general","dollar-general",text)
    text = re.sub(r"dollar ge","dollar-general",text)
    text = re.sub(r"dollar tree","dollar-tree",text)
    text = re.sub(r"family dollar","family-dollar",text)
    text = re.sub(r"circle k","circle-k",text)
    text = re.sub(r"taco bell","taco-bell",text)
    text = re.sub(r"burger king","burger-king",text)
    text = re.sub(r"365 market","365-market",text)
    text = re.sub("[\']", "", text)
    # TODO: Investigate merchant collisions (doordash*mcdonalds, etc)
    # TODO: Investigate what NNT is
    return text

class DescriptionCleanFlow(FlowSpec):
    """
    A flow to clean the transaction descriptions

    This flow performs the following steps:
    1) Carries out a set of data cleaning operations to improve the chances
    of correctly categorizing merchants.
    """

    @step
    def start(self):
        """
        The start step:
        1) Loads the data into a pandas dataframe
        2) Leverages the re package to clean up the transaction descriptions

        """
        import pandas as pd

        run = Flow('TransactionLoadFlow').latest_successful_run
        raw_df = run.data.df

        raw_df["cleaned_description"] = raw_df["description"].apply(clean_text)
        self.cleaned_df = raw_df[["merchantName","description",
                                  "cleaned_description"]]
        self.next(self.end)

    @step
    def end(self):
        """
        The end step:
        1) Prints acknowledgement of end.
        """
        print("DescriptionCleanFlow is complete")

if __name__ == "__main__":
    DescriptionCleanFlow()
