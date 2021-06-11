# Code for *How Is Earnings News Transmitted to Stock Prices?*
Journal of Accounting Research

[Vincent Grégoire](http://www.vincentgregoire.com/) (HEC Montréal) and [Charles Martineau](https://www.charlesmartineau.com/) (University of Toronto)

The latest version of this code can be found at [https://github.com/vgreg/earnings_news_jar](https://github.com/vgreg/earnings_news_jar).

*Note*: this document is written in Github Flavored Markdown. It can be read by any text editor, but is best viewed with a GFM viewer.

For any question, please contact [Vincent Grégoire](mailto:vincent.3.gregoire@hec.ca).
## Nasdaq ITCH

We have released our code for processing the Nasdaq ITCH data as an open-source Python 3
module available on [GitHub](https://github.com/vgreg/MeatPy). The documention is available on [Read The Docs](https://meatpy.readthedocs.io/en/latest/).

The documentation includes sample code to extract the metrics used in the paper.
## Thomson Reuters Tick History

The raw Thomson Reuters Tick History data (now Refinitv Tick History) was provided to us directly
by the [Securities Industry Research Centre of Asia-Pacific](https://www.sirca.org.au/) (SIRCA) on hard drives.
The dataset used in this paper is about 12TB in size (gzip-compressed CSV files) and required processing
on dedicated computing servers.
There is one daily TAS (time and sales file) per listing exchange. Our code first seperates
trades and quote in different files.

Because a good part of our code is server-specific (the part that loops over all files for processing
concurrently), for clarity we only provide the actual functions that execute the data cleaning and
processing.

The TRTH processing was done over the course of two years, and thus some code is in Python 2 format
while some code is in Python 3. The header comments in each file gives the necessary details.

### Code

The included Python files and their recommended execution order are:

**For trades:**

1. `ExtractTrades.py`
2. `ClassifyTrades.py`
3. `AlignDates.py`
4. `ExtractTradesAroundEarnings.py`
5. `ExtractTradesEarningsDescriptiveStats.py`
6. `ExtractTradesAfterEarningsResample.py`
7. `ExtractTradesAfterNewsBeforeOpen.py`

**For quotes:**

1. `ExtractQuotes.py`
2. `ExtractQuotesAroundEarnings.py`
3. `ExtractQuotesAfterEarningsResample.py`

## Sample stocks

We also include the sample of stocks used in our analysis for the Journal of Accounting Research with their corresponding CRSP-PERMNO and TRTH identifier in a csv file.
