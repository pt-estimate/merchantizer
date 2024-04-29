from metaflow import Flow, FlowSpec, step
import re

# We define a number of merchants that we know are in the documents
# These are ordered by importance and when there are multiple, the first
# takes precendence.
ZERO_SHOT_TOPIC_LIST = ["square","cashapp","toast","doordash",
                        "target","walmart","mcdonalds","apple","uber",
                        "google","amazon","shell","circle-k","7-eleven",
                        "dollar-general","dollar-tree","family-dollar",
                        "lyft","taco-bell","exxon","starbucks","chevron",
                        "wendys","365-market"]
#ZERO_SHOT_TOPIC_LIST = ["doordash"]

def clean_description(description):
    """
    Make description lowercase,
    replace amazon variations (amzn) with amazon,
    replace wal-mart variations (wal mart/wm supercenter) with wal-mart,
    clean up the dollar store variations (dollar tree/dollar general/
    family dollar),
    add dashes between some brands to differentiate them from words,
    expand description for doordash/square/paypal to make them more expressive,
    remove backslashes
    """
    description = description.lower()
    description = re.sub(r"amzn\w*","amazon ",description)
    description = re.sub(r"wal mart","wal-mart ",description)
    description = re.sub(r"wm supercenter","wal-mart ",description)
    description = re.sub(r"wal","wal-mart ",description)
    description = re.sub(r"sq\w*","square ",description)
    description = re.sub(r"cash app","cashapp ",description)
    description = re.sub(r"dd\w*","doordash ",description)
    description = re.sub(r"tst*","toast ",description)
    description = re.sub(r".com\w*"," ",description)
    description = re.sub(r"qt\w*","non-merchant",description)
    description = re.sub(r"dollar general","dollar-general",description)
    description = re.sub(r"dollar ge","dollar-general",description)
    description = re.sub(r"dollar tree","dollar-tree",description)
    description = re.sub(r"family dollar","family-dollar",description)
    description = re.sub(r"circle k","circle-k",description)
    description = re.sub(r"taco bell","taco-bell",description)
    description = re.sub(r"burger king","burger-king",description)
    description = re.sub(r"365 market","365-market",description)
    description = re.sub("[\']", "", description)
    # TODO: Investigate what NNT is
    return description

def prune_merchants(clean_description):
    """
    An approach to merchant collisions. If a zero-shot merchant exists in
    the description, return it. Give precedence to zero-shot merchants that
    appear first (left-most) in the description.
    """
    for text in clean_description.split():
        if text in ZERO_SHOT_TOPIC_LIST:
            return text
        else:
            return clean_description

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

        raw_df["cleaned_description"] = raw_df["description"] \
                                              .apply(clean_description)
        raw_df["pruned_description"] = raw_df["cleaned_description"] \
                                             .apply(prune_merchants)
        self.cleaned_df = raw_df[["merchantName","description",
                                  "cleaned_description","pruned_description"]]
        self.zs_topic_list = ZERO_SHOT_TOPIC_LIST
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
