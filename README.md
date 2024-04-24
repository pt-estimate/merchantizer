## merchantizer
Given a noisy set of transaction descriptions, leverage ML to predict a given transactions merchant

### Project Structure

There are three parts to this work, each with a .py file where the work is carried out, and a jupyter notebook. I recommend walking through the notebooks first as commentary on decisions made. Then use the python code to dig in more.
* load - Data download and initial data discovery
* clean - Data cleaning in preparation for modeling
* model - Data prep in support of modeling and merchant categorization

#### Prerequisites
homebrew
python 3
pipx
poetry

#### Installing
```zsh
brew install pipx
pipx install poetry
```

Clone this github repo

From within the downloaded repo, run the following to load the data for exploration.

```zsh
poetry shell
python merchantizer/load/load.py run
jupyter-notebook merchantizer/load_discussion.ipynb
```

### Built With
* [Poetry](https://python-poetry.org) - Dependency Management & Packaging
* [Metaflow](https://metaflow.org) - Machine Learning Framework
