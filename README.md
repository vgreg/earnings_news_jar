# Code for *How Is Earnings News Transmitted to Stock Prices?*
Journal of Accounting Research

[Vincent Grégoire](http://www.vincentgregoire.com/) (HEC Montréal) and [Charles Martineau](https://www.charlesmartineau.com/) (University of Toronto)

The latest version of this code can be found at [https://github.com/vgreg/earnings_news_jar](https://github.com/vgreg/earnings_news_jar).

*Note*: this document is written in Github Flavored Markdown. It can be read by any text editor, but is best viewed with a GFM viewer.

For questions related to RavenPack and IBES, please contact [Charles Martineau](mailto:charles.martineau@utoronto.ca).
For questions related to Nasdaq ITCH or TRTH, please contact [Vincent Grégoire](mailto:vincent.3.gregoire@hec.ca).


## Ravenpack

We provide a Jupyter notebook with Python sample code (RavenPack.ipynb) on how to extract earnings announcement timestamps and analyst recommendation revision news from Ravenpack.

## IBES

We provide a Jupyter notebook with Python sample code (IBES_Data_Processing_JAR.ipynb) to retrieved earnings announcement dates and analyst earnings surprises from IBES.

IBES_Data_Processing_JAR.ipynb

## Sample stocks

We include the sample of stocks used in our analysis for the Journal of Accounting Research with their corresponding CRSP-PERMNO and TRTH identifier in a csv file.

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
concurrently), for clarity we only provide the actual functions for data cleaning and
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

## NYSE Trade and Quote (TAQ)

*Note*: We do not use TAQ in the paper. We provide this information to facilitate replication for researchers that wish to use TAQ.

While TRTH is more comprehensive, the NYSE Trade and Quote (TAQ) dataset is more commonly used in academic research. Because our study focuses on trades and \nbbo quotes, both products can be used interchangeably to produce our results. Indeed, TRTH sources SIP data (the consolidated feed used to reconstruct the NBBO) from the NYSE. We have manually verified a few events and confirmed that trades and quotes updates are a perfect match. One aspect on which they differ is in the ease of use for researchers. While TRTH provides the NBBO in a simple dataset, the NBBO must be constructed manually from TAQ data by properly merging the quote and nbbo tables.  

To use TAQ in order to replicate our methodology, we recommend using the sample code from [Holden, Craig W., and Stacey Jacobsen. "Liquidity measurement problems in fast, competitive markets: Expensive and cheap solutions." *The Journal of Finance* 69, no. 4 (2014): 1747-1785](https://doi.org/10.1111/jofi.12127) available on [Professor Craig Holden's personal website](https://kelley.iu.edu/cholden/) and [Professor Stacey Jacobsen's website](https://www.smu.edu/cox/Our-People-and-Community/Faculty/Stacey-Jacobsen). However, because that code was designed to work for regular hours, changes must be made. Researchers must change the valid time range to include after-hours trades and quotes which are excluded by default and apply the following changes to the sample code:
1. Trades

    Exclude trades that have one of the following conditions under the field name `tr_scon`:
    - `L`: Sold Last (Late Reporting)
    - `P`: Prior Reference Price
    - `U`: Extended Hours (Sold Out of Sequence)
    - `Z`: Sold (Out of Sequence)
    - `4`: Derivatively Priced

After-hours trades are identified under the condition `T`.

2. Quotes

    The main issue in dealing with quotes in the after-hours market is that liquidity is very limited, spreads are wide and there are often no quotes at all. The sample code from Holden & Jacobsen (2014), which was designed for regular market hours, treats most of these conditions as errors and removes problematic quote updates. This is not appropriate in the after-hours because by removing quote updates we are assuming that the old quotes are still valid, which inflates the available liquidity. In our context, it is thus better to keep potentially problematic quote updates than to discard them.

    - Add `C` (closing) to the list of quotes condition under the field name `qu_cond`.

    - In step 2, do not remove entries for which there are no quotes. This is important because "empty" quotes are common in the after-hours market. Thus, it is important to remove the following lines (lines 155-159) from Holden's sample code:

            /* if both ask and bid are set to 0 or . then delete */
            if Best_Ask le 0 and Best_Bid le 0 then delete;
            if Best_Asksiz le 0 and Best_Bidsiz le 0 then delete;
            if Best_Ask = . and Best_Bid = . then delete;
            if Best_Asksiz = . and Best_Bidsiz = . then delete;

    - Skip step 3 in the sample code. We do not want to exclude wide bid-ask spreads at this stage (we do exclude them from some tests). Wide spreads is the norm in the after-hours market and so should not be automatically flagged as ``erroneous''.

    - In step 5, do not delete crossed markets, do not delete abnormal spreads, and do no delete withdrawn quotes. Therefore, remove the following lines (lines 250-264) in the code provided by Holden & Jacobsen (2014):

            /* Delete if abnormal crossed markets */
            if Bid>Ask then delete;

            /* Delete abnormal spreads*/
            if Spread>5 then delete;

            /* Delete withdrawn Quotes. This is
            when an exchange temporarily has no quote, as indicated by quotes
            with price or depth fields containing values less than or equal to 0
            or equal to '.'. See discussion in Holden and Jacobsen (2014),
            page 11. */
            if Ask le 0 or Ask =. then delete;
            if Asksiz le 0 or Asksiz =. then delete;
            if Bid le 0 or Bid =. then delete;
            if Bidsiz le 0 or Bidsiz =. then delete;
