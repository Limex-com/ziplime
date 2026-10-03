"""Shared settings for the Hugging Face point-in-time dataset examples."""
import datetime
from pathlib import Path

ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

TRADING_CALENDAR = "XNYS"
BUNDLE_NAME = "hf_equities_daily"

#: Equities to price the strategies with. Yahoo Finance, no credentials.
#:
#: These five are the names Congress discloses most often, which is what makes the examples show
#: anything: a dataset row that lands on a ticker the backtest does not hold is invisible.
EQUITY_TICKERS = ["MSFT", "NVDA", "AAPL", "AMZN", "META"]
#: All five list on Nasdaq Global Select. Naming the MIC matters: META is also listed on ARCX,
#: and resolving by ticker alone would have to pick one of the two.
EQUITY_MIC = "XNGS"

#: The universe the congressional strategies trade, as (ticker, MIC) pairs.
#:
#: The sixty most-disclosed tickers that resolve to exactly one US listing, which together cover
#: 23% of all transaction-report rows. Sixty rather than a handful on purpose: a universe of five
#: mega-cap technology names decides the answer before the strategy runs, and this one carries
#: banks, energy, healthcare and staples that behaved very differently over the window.
#:
#: Every one is named with its venue. A bare ticker is not unique in this database: `T` also
#: resolves to a Moscow listing, `META` to both Nasdaq and NYSE Arca. Resolving by name alone
#: picks one silently, and not necessarily the one the price bundle holds.
CONGRESS_UNIVERSE = [
    ("MSFT", "XNGS"), ("AAPL", "XNGS"), ("AMZN", "XNGS"), ("NVDA", "XNGS"),
    ("GOOGL", "XNGS"), ("JPM", "XNYS"), ("UNH", "XNYS"), ("V", "XNYS"),
    ("T", "XNYS"), ("WFC", "XNYS"), ("DIS", "XNYS"), ("JNJ", "XNYS"),
    ("HD", "XNYS"), ("NTAP", "XNGS"), ("PYPL", "XNGS"), ("CMCSA", "XNGS"),
    ("GOOG", "XNGS"), ("VZ", "XNYS"), ("INTC", "XNGS"), ("ADBE", "XNGS"),
    ("ACN", "XNYS"), ("PG", "XNYS"), ("XOM", "XNYS"), ("CVX", "XNYS"),
    ("MRK", "XNYS"), ("PFE", "XNYS"), ("BAC", "XNYS"), ("NFLX", "XNGS"),
    ("PEP", "XNGS"), ("C", "XNYS"), ("SBUX", "XNGS"), ("GE", "XNYS"),
    ("TXN", "XNGS"), ("TSLA", "XNGS"), ("CVS", "XNYS"), ("TMO", "XNYS"),
    ("KO", "XNYS"), ("MDT", "XNYS"), ("IBM", "XNYS"), ("ABT", "XNYS"),
    ("COST", "XNGS"), ("ABBV", "XNYS"), ("SCHW", "XNYS"), ("LLY", "XNYS"),
    ("QCOM", "XNGS"), ("UPS", "XNYS"), ("NEE", "XNYS"), ("TJX", "XNYS"),
    ("FDX", "XNYS"), ("ORCL", "XNYS"), ("MA", "XNYS"), ("CSCO", "XNGS"),
    ("AVGO", "XNGS"), ("INTU", "XNGS"), ("BMY", "XNYS"), ("MS", "XNYS"),
    ("SLB", "XNYS"), ("DHR", "XNYS"), ("CRM", "XNYS"), ("META", "XNGS"),
]

#: Window for the congressional strategies. Starts in 2016: the House catalog is thin before then
#: -- roughly 60% of 2014 filings were scanned paper that no extractor has read.
CONGRESS_START = datetime.date(2016, 1, 4)
CONGRESS_END = datetime.date(2026, 8, 31)

#: The committee whose members `h04` follows. House Armed Services is the most active in this
#: data: 20 449 transaction reports from its members, against 6 711 for Financial Services.
COMMITTEE_ID = "HSAS"

#: Window for the congress examples. The dataset runs 2012-01-25 to 2026-08-24.
START = datetime.date(2023, 1, 1)
END = datetime.date(2026, 8, 31)

#: Window for the insider examples. That dataset stops at 2016-03-02, so its strategies run
#: earlier; mounting it over the window above would fetch partitions holding nothing.
INSIDER_START = datetime.date(2012, 1, 1)
INSIDER_END = datetime.date(2014, 12, 31)

STARTING_CASH = 1_000_000.0

#: Datasets these examples mount, pinned so the numbers in the README stay reproducible.
#:
#: Passing a commit rather than a branch is the whole point of pinning: these datasets are
#: append-only and grow, so `main` today is not `main` next month, and an unpinned backtest
#: quietly stops being comparable with the one you ran before.
CONGRESS_DATASET = "ZipLime/congress-trading"
CONGRESS_REVISION = "cb3c6896"
INSIDER_DATASET = "ZipLime/insider-trading"
INSIDER_REVISION = "ba0785efcede0b3a13af48dc658a1d39bc87ad1e"


# ---------------------------------------------------------------------------------------------
# The insider-trading strategy suite (i00-i10).
# ---------------------------------------------------------------------------------------------

#: Window. The dataset covers 2006-01-03 to 2026-06-30 without a missing month; this takes the
#: most recent decade of it, which is where the price history is densest.
INSIDER10_START = datetime.date(2016, 1, 4)
INSIDER10_END = datetime.date(2026, 6, 30)

INSIDER_REVISION_CURRENT = "a1ef3c9e"

#: The universe: names whose insiders actually trade.
#:
#: Chosen by cluster-buy days over 2016-2026 among tickers with at least 20 disclosed open-market
#: purchases, then narrowed to those resolving to exactly one US listing with a usable Yahoo
#: history. Mostly regional banks and micro caps, which is not an accident -- insider buying is
#: concentrated in small companies, and that is where the literature finds whatever signal exists.
#:
#: **This list survives.** Of 251 candidates, 84 no longer return data from Yahoo at all
#: ("possibly delisted") and 42 more lack a long enough history: 126 of 251 dropped out, and the
#: ones that died are precisely those where insider buying did not work. Absolute returns from any
#: backtest on this universe are therefore biased upward, and badly. The control (`i00`) holds the
#: same 125 names, so it carries the identical bias and comparisons against it stay meaningful
#: even though the absolute numbers do not.
INSIDER10_UNIVERSE = [
    ("GABC", "XNGS"), ("ED", "XNYS"), ("SYBT", "XNGS"), ("MMLP", "XNGS"),
    ("YORW", "XNGS"), ("TEX", "XNYS"), ("OPK", "XNGS"), ("BH", "XNYS"),
    ("MCHX", "XNGS"), ("HY", "XNYS"), ("GEG", "XNGS"), ("BUSE", "XNGS"),
    ("MTDR", "XNYS"), ("BATRA", "XNGS"), ("CWH", "XNYS"), ("FUNC", "XNGS"),
    ("TPL", "XNYS"), ("SSP", "XNGS"), ("RM", "XNYS"), ("RUN", "XNGS"),
    ("COTY", "XNYS"), ("TLYS", "XNYS"), ("CTRN", "XNGS"), ("APO", "XNYS"),
    ("BW", "XNYS"), ("FFIN", "XNGS"), ("NOG", "XNYS"), ("DKL", "XNYS"),
    ("SXT", "XNYS"), ("TRN", "XNYS"), ("FLWS", "XNGS"), ("SHEN", "XNGS"),
    ("CBAN", "XNYS"), ("FSK", "XNYS"), ("PFSI", "XNYS"), ("EPD", "XNYS"),
    ("FSTR", "XNGS"), ("IFF", "XNYS"), ("CRMT", "XNGS"), ("BBW", "XNYS"),
    ("PRTS", "XNGS"), ("CODI", "XNYS"), ("EARN", "XNYS"), ("DXLG", "XNGS"),
    ("ASPS", "XNGS"), ("PRPL", "XNGS"), ("LAB", "XNGS"), ("ADC", "XNYS"),
    ("CARE", "XNGS"), ("ITRI", "XNGS"), ("SPOK", "XNGS"), ("FFBC", "XNGS"),
    ("TRST", "XNGS"), ("GPMT", "XNYS"), ("UMH", "XNYS"), ("CPIX", "XNGS"),
    ("NNBR", "XNGS"), ("UFI", "XNYS"), ("CCNE", "XNGS"), ("HRTG", "XNYS"),
    ("CLF", "XNYS"), ("HTLD", "XNGS"), ("NRIM", "XNGS"), ("CAC", "XNGS"),
    ("UAN", "XNYS"), ("CHCO", "XNGS"), ("CULP", "XNYS"), ("LCUT", "XNGS"),
    ("ENR", "XNYS"), ("PGC", "XNGS"), ("FNKO", "XNGS"), ("COGT", "XNGS"),
    ("TNET", "XNYS"), ("TISI", "XNYS"), ("STRR", "XNGS"), ("LXRX", "XNGS"),
    ("BY", "XNYS"), ("CSV", "XNYS"), ("TKO", "XNYS"), ("HWBK", "XNGS"),
    ("FBIZ", "XNGS"), ("INCY", "XNGS"), ("NXRT", "XNYS"), ("FNB", "XNYS"),
    ("NDLS", "XNGS"), ("PETS", "XNGS"), ("NSIT", "XNGS"), ("ANGI", "XNGS"),
    ("GTES", "XNYS"), ("TBBK", "XNGS"), ("RRGB", "XNGS"), ("SCOR", "XNGS"),
    ("SWX", "XNYS"), ("VST", "XNYS"), ("SVVC", "XNGS"), ("CVNA", "XNYS"),
    ("BBDC", "XNYS"), ("CQP", "XNYS"), ("FPI", "XNYS"), ("MPAA", "XNGS"),
    ("SAFE", "XNYS"), ("MYE", "XNYS"), ("THFF", "XNGS"), ("FCNCA", "XNGS"),
    ("CVI", "XNYS"), ("BG", "XNYS"), ("UTI", "XNYS"), ("GRPN", "XNGS"),
    ("SNX", "XNYS"), ("ATEC", "XNGS"), ("UMBF", "XNGS"), ("BNED", "XNYS"),
    ("STX", "XNGS"), ("SFNC", "XNGS"), ("SHBI", "XNGS"), ("CCO", "XNYS"),
    ("OLN", "XNYS"), ("RVSB", "XNGS"), ("COLB", "XNGS"), ("CWEN", "XNYS"),
    ("GOGO", "XNGS"), ("AP", "XNYS"), ("MRBK", "XNGS"), ("CLBK", "XNGS"),
    ("HLF", "XNYS"),
]


# ---------------------------------------------------------------------------------------------
# The fundamentals suite (f00-f03), on ZipLime/company-fundamentals.
# ---------------------------------------------------------------------------------------------

FUNDAMENTALS_DATASET = "ZipLime/company-fundamentals"

#: Window. XBRL tagging begins in 2009 Q1 and small filers only arrive from 2012, so the corpus is
#: thin before then; this takes the decade where coverage is stable.
FUNDAMENTALS_START = datetime.date(2013, 1, 2)
FUNDAMENTALS_END = datetime.date(2026, 6, 30)

#: The dataset has **no ticker column** -- its identifier is the issuer CIK, and the README says so
#: explicitly, calling a CIK-to-ticker map "a separate problem with its own point-in-time trap".
#: It is right. This map is built from ZipLime/insider-trading, which carries both keys, by taking
#: the most recently filed ticker per CIK.
#:
#: That last-known-ticker rule is itself look-ahead: a company that changed symbol in 2020 is
#: labelled here with the symbol it uses now, and the price series fetched under that symbol is the
#: post-change one. For the names below -- large, long-listed, continuously reporting -- symbol
#: changes are rare, but the bias is real and this is where it enters.
CIK_TO_TICKER = {
    "0001433270": "AR", "0001521951": "FBIZ", "0001627223": "CC", "0000711377": "NEOG",
    "0001157601": "MDGL", "0000072573": "MOV", "0000066382": "MLKN", "0000314203": "MUX",
    "0001227654": "CMP", "0000918646": "EXP", "0001280058": "BLKB", "0000891166": "UVE",
    "0001634117": "BNED", "0001156039": "ELV", "0001645113": "NVCR", "0000060714": "LXU",
    "0001110803": "ILMN", "0001022671": "STLD", "0001101302": "ENTG", "0000023197": "CMTL",
    "0000108385": "WRLD", "0001395942": "OPLN", "0000018255": "CATO", "0000315374": "HURC",
    "0000356309": "NJR", "0000912728": "FWRD", "0000876523": "EZPW", "0000089800": "SHW",
    "0001043277": "CHRW", "0001361658": "TNL", "0001177609": "FIVE", "0001175454": "CPAY",
    "0000091767": "SON", "0001424929": "FOXF", "0000104889": "GHC", "0000912767": "UFPI",
    "0000100726": "UFI", "0001138723": "ARAY", "0000104894": "ELME", "0001308208": "ULH",
    "0000110621": "RPM", "0001087294": "CPIX", "0000935703": "DLTR", "0001022408": "PLUS",
    "0001041514": "LSAK", "0000040704": "GIS", "0001408198": "MSCI", "0001501989": "CTMX",
    "0000701985": "BBWI", "0000880117": "JBSS", "0000832988": "SIG", "0000057515": "MZTI",
    "0000898437": "ANIK", "0001500217": "AAT", "0001497770": "WD", "0000215466": "CDE",
    "0001436126": "MG", "0000896156": "ETD", "0000825542": "SMG", "0001145986": "ASPN",
    "0001410636": "AWK", "0000712770": "OLP", "0001636282": "SYRE", "0001467858": "GM",
    "0000355948": "RELL", "0001509991": "KOS", "0000078128": "WTRG", "0000866729": "SCHL",
    "0000067716": "MDU", "0001466026": "MSBI", "0000918251": "MPAA", "0001158449": "AAP",
    "0001000209": "MFIN", "0001009829": "JAKK", "0000745732": "ROST", "0000080420": "POWL",
    "0000037996": "F", "0000946673": "BANR", "0000907254": "BFS", "0001130144": "BSRR",
    "0000910521": "DECK", "0001144215": "AYI", "0001230245": "PIPR", "0000056873": "KR",
    "0000027904": "DAL", "0001056903": "AWR", "0001321732": "PEN", "0001050915": "PWR",
    "0000037785": "FMC", "0000004127": "SWKS", "0001108524": "CRM", "0000354190": "AJG",
    "0000723254": "CTAS", "0001031296": "FE", "0000063276": "MAT", "0000018498": "GCO",
    "0000835011": "MGPI", "0000024090": "CIA", "0001610250": "BOOT", "0001099590": "MELI",
    "0001069183": "AXON", "0000859070": "FCBC", "0001060391": "RSG", "0000082020": "USLM",
    "0001332551": "ACR", "0000069633": "NSSC", "0001035092": "SHBI", "0001579298": "BURL",
    "0000108516": "WOR", "0001345126": "CODI", "0001018724": "AMZN", "0001116132": "TPR",
    "0000895417": "ELS", "0001467761": "FIEE", "0000014930": "BC", "0000091419": "SJM",
    "0001041368": "RVSB", "0000707179": "ONB", "0000106640": "WHR", "0000315293": "AON",
    "0001014739": "OPCH", "0000056679": "KFY", "0001067294": "CBRL", "0001165002": "WHG",
    "0001520697": "ACHC", "0000719955": "WSM", "0001326380": "GME", "0001265131": "HTH",
    "0001170010": "KMX", "0001423221": "NX", "0000706698": "UTMD", "0000763744": "LCII",
    "0000794367": "M", "0000845877": "AGM", "0000056978": "KLIC", "0000730272": "RGEN",
    "0001403568": "ULTA", "0000715957": "D", "0000926423": "MIND", "0000907471": "CASH",
    "0000723603": "CULP", "0001080014": "INVA", "0000080172": "NPK", "0001446847": "IRWD",
    "0000357294": "HOV", "0001436425": "HBCP", "0001011060": "NORD", "0000936340": "DTE",
    "0001406587": "FOR", "0000812011": "MTN", "0001633978": "LITE", "0001163370": "NRIM",
    "0001569187": "AHRT", "0001050446": "MSTR", "0000726958": "CASY", "0000717954": "UNF",
    "0001520006": "MTDR", "0000887905": "LTC", "0000009326": "BCPC", "0000926282": "ADTN",
    "0001632127": "CABO", "0001474903": "BGSF", "0001459200": "ALRM", "0000746838": "UIS",
    "0001624794": "CSW", "0000921557": "RBCAA", "0000072331": "NDSN", "0001318220": "WCN",
    "0000915779": "DAKT", "0001529377": "ACRE", "0001096752": "EPC", "0000895456": "RCKY",
    "0000813298": "DXLG", "0000916365": "TSCO", "0000319201": "KLAC", "0001021635": "OGE",
    "0001050743": "PGC", "0001730984": "BCML", "0000703351": "EAT", "0001467373": "ACN",
    "0001466085": "IRT", "0001365135": "WU", "0001157647": "WNEB", "0001267565": "COLL",
    "0001192448": "GKOS", "0001400810": "HCI", "0000842023": "TECH", "0000037472": "FLXS",
    "0000889900": "PTEN", "0001012019": "RUSHA", "0000765207": "FNLC", "0001571776": "CHMI",
    "0001057379": "HCKT", "0000882184": "DHI", "0001598665": "HRTG", "0001486957": "BWXT",
    "0000093556": "SWK", "0001337298": "FF", "0000769397": "ADSK", "0000106040": "WDC",
    "0000766829": "HTO", "0001055160": "MFA", "0001434647": "ZVRA", "0000712034": "ACCO",
    "0000799167": "MRTN", "0001005286": "LFCR", "0000752714": "MGRC", "0001441236": "CLW",
    "0000879526": "WNC", "0000794170": "TOL", "0001445305": "WK", "0001730168": "AVGO",
    "0000079282": "BRO", "0001405495": "IDCC", "0000793952": "HOG", "0000007084": "ADM",
    "0000945841": "POOL", "0001039684": "OKE", "0000729986": "UBSI", "0001495320": "VRA",
    "0000818479": "XRAY", "0001411579": "AMC", "0000785956": "JJSF", "0001613103": "MDT",
    "0000277948": "CSX", "0000840489": "FCFS", "0001224608": "CNO", "0000012659": "HRB",
    "0000027419": "TGT", "0000911177": "CWST",
}

#: 230 issuers: those reporting the fields these strategies need most completely, that map to
#: exactly one US listing and have a usable price history. Survivorship applies here as everywhere
#: else in this directory -- the control (`f00`) holds the same names and carries the same bias.
FUNDAMENTALS_UNIVERSE = [
    ("SYRE", "XNGS"), ("CASH", "XNGS"), ("NVCR", "XNGS"), ("AMZN", "XNGS"),
    ("BNED", "XNYS"), ("TGT", "XNYS"), ("UVE", "XNYS"), ("MELI", "XNGS"),
    ("FNLC", "XNGS"), ("CSX", "XNGS"), ("FIVE", "XNGS"), ("FF", "XNYS"),
    ("ADTN", "XNGS"), ("WNC", "XNYS"), ("NORD", "XNYS"), ("CMP", "XNYS"),
    ("LFCR", "XNGS"), ("KR", "XNYS"), ("AWK", "XNYS"), ("GME", "XNYS"),
    ("UTMD", "XNGS"), ("UBSI", "XNGS"), ("HRTG", "XNYS"), ("CHMI", "XNYS"),
    ("ELS", "XNYS"), ("POOL", "XNGS"), ("NRIM", "XNGS"), ("D", "XNYS"),
    ("BGSF", "XNYS"), ("CPIX", "XNGS"), ("FLXS", "XNGS"), ("OKE", "XNYS"),
    ("CRM", "XNYS"), ("BANR", "XNGS"), ("NSSC", "XNGS"), ("ACHC", "XNGS"),
    ("POWL", "XNGS"), ("SWK", "XNYS"), ("PIPR", "XNYS"), ("COLL", "XNGS"),
    ("BCPC", "XNGS"), ("IRT", "XNYS"), ("HCKT", "XNGS"), ("BWXT", "XNYS"),
    ("FBIZ", "XNGS"), ("AAT", "XNYS"), ("FMC", "XNYS"), ("MSTR", "XNGS"),
    ("BURL", "XNYS"), ("ULTA", "XNGS"), ("PLUS", "XNGS"), ("MDGL", "XNGS"),
    ("NEOG", "XNGS"), ("CULP", "XNYS"), ("GCO", "XNYS"), ("XRAY", "XNGS"),
    ("WNEB", "XNGS"), ("ELME", "XNYS"), ("JBSS", "XNGS"), ("IRWD", "XNGS"),
    ("DECK", "XNYS"), ("CPAY", "XNYS"), ("CODI", "XNYS"), ("ELV", "XNYS"),
    ("CIA", "XNYS"), ("ACN", "XNYS"), ("ACRE", "XNYS"), ("OLP", "XNYS"),
    ("CMTL", "XNGS"), ("FIEE", "ARCX"), ("NPK", "XNYS"), ("UFPI", "XNGS"),
    ("MLKN", "XNGS"), ("BBWI", "XNYS"), ("CDE", "XNYS"), ("SIG", "XNYS"),
    ("EZPW", "XNGS"), ("RSG", "XNYS"), ("ROST", "XNGS"), ("AAP", "XNYS"),
    ("HRB", "XNYS"), ("PGC", "XNGS"), ("WOR", "XNYS"), ("NX", "XNYS"),
    ("RELL", "XNGS"), ("LSAK", "XNGS"), ("SHBI", "XNGS"), ("SHW", "XNYS"),
    ("WSM", "XNYS"), ("ENTG", "XNGS"), ("MGPI", "XNGS"), ("BSRR", "XNGS"),
    ("MDU", "XNYS"), ("M", "XNYS"), ("AJG", "XNYS"), ("ARAY", "XNGS"),
    ("LCII", "XNYS"), ("VRA", "XNGS"), ("KOS", "XNYS"), ("AHRT", "XNYS"),
    ("BFS", "XNYS"), ("ULH", "XNGS"), ("FCFS", "XNGS"), ("JJSF", "XNGS"),
    ("BRO", "XNYS"), ("EXP", "XNYS"), ("PEN", "XNYS"), ("HCI", "XNYS"),
    ("WHG", "XNYS"), ("LTC", "XNYS"), ("AYI", "XNYS"), ("CTAS", "XNGS"),
    ("AGM", "XNYS"), ("HBCP", "XNGS"), ("USLM", "XNGS"), ("TPR", "XNYS"),
    ("MSCI", "XNYS"), ("CBRL", "XNGS"), ("CSW", "XNYS"), ("BCML", "XNGS"),
    ("AVGO", "XNGS"), ("BC", "XNYS"), ("ADSK", "XNGS"), ("SMG", "XNYS"),
    ("AXON", "XNGS"), ("RUSHA", "XNGS"), ("UIS", "XNYS"), ("TECH", "XNGS"),
    ("ILMN", "XNGS"), ("WRLD", "XNGS"), ("RGEN", "XNGS"), ("AWR", "XNYS"),
    ("MUX", "XNYS"), ("MFA", "XNYS"), ("KFY", "XNYS"), ("STLD", "XNGS"),
    ("SON", "XNYS"), ("AMC", "XNYS"), ("WK", "XNYS"), ("MAT", "XNGS"),
    ("FCBC", "XNGS"), ("CATO", "XNYS"), ("MFIN", "XNGS"), ("OGE", "XNYS"),
    ("MTN", "XNYS"), ("MIND", "XNGS"), ("TNL", "XNYS"), ("WCN", "XNYS"),
    ("WHR", "XNYS"), ("NJR", "XNYS"), ("CHRW", "XNGS"), ("DXLG", "XNGS"),
    ("HURC", "XNGS"), ("MTDR", "XNYS"), ("ADM", "XNYS"), ("GM", "XNYS"),
    ("DAL", "XNYS"), ("MGRC", "XNGS"), ("ACR", "XNYS"), ("CWST", "XNGS"),
    ("WTRG", "XNYS"), ("CABO", "XNYS"), ("GKOS", "XNYS"), ("WD", "XNYS"),
    ("SJM", "XNYS"), ("LXU", "XNYS"), ("SCHL", "XNGS"), ("UNF", "XNYS"),
    ("F", "XNYS"), ("FWRD", "XNGS"), ("MG", "XNYS"), ("CTMX", "XNGS"),
    ("DHI", "XNYS"), ("RVSB", "XNGS"), ("SWKS", "XNGS"), ("RCKY", "XNGS"),
    ("NDSN", "XNGS"), ("ANIK", "XNGS"), ("BLKB", "XNGS"), ("HTH", "XNYS"),
    ("ALRM", "XNGS"), ("ZVRA", "XNGS"), ("CC", "XNYS"), ("IDCC", "XNGS"),
    ("OPCH", "XNGS"), ("HTO", "XNGS"), ("FE", "XNYS"), ("WDC", "XNGS"),
    ("ACCO", "XNYS"), ("TOL", "XNYS"), ("KLAC", "XNGS"), ("MPAA", "XNGS"),
    ("DTE", "XNYS"), ("TSCO", "XNGS"), ("MDT", "XNYS"), ("CNO", "XNYS"),
    ("GHC", "XNYS"), ("CASY", "XNGS"), ("HOG", "XNYS"), ("MSBI", "XNGS"),
    ("WU", "XNYS"), ("EPC", "XNYS"), ("KLIC", "XNGS"), ("DAKT", "XNGS"),
    ("BOOT", "XNYS"), ("JAKK", "XNGS"), ("MOV", "XNYS"), ("RBCAA", "XNGS"),
    ("PTEN", "XNGS"), ("HOV", "XNYS"), ("GIS", "XNYS"), ("LITE", "XNGS"),
    ("AON", "XNYS"), ("UFI", "XNYS"), ("EAT", "XNYS"), ("KMX", "XNYS"),
    ("DLTR", "XNGS"), ("MRTN", "XNGS"), ("PWR", "XNYS"), ("MZTI", "XNGS"),
    ("OPLN", "XNYS"), ("CLW", "XNYS"), ("ASPN", "XNYS"), ("FOXF", "XNGS"),
    ("FOR", "XNYS"), ("ONB", "XNGS"), ("INVA", "XNGS"), ("ETD", "XNYS"),
    ("RPM", "XNYS"), ("AR", "XNYS"),
]

# ---------------------------------------------------------------------------------------------
# The earnings suite (e01-e03), on ZipLime/earnings-calendar.
# ---------------------------------------------------------------------------------------------

EARNINGS_DATASET = "ZipLime/earnings-calendar"

#: The same 230 names and the same window as the fundamentals suite, deliberately. The earnings
#: calendar keys on the issuer CIK and has no ticker column, so it needs `CIK_TO_TICKER` exactly
#: as the fundamentals do -- and sharing the universe means `f00` is already the right control:
#: same names, same decade, same survivorship bias, reading no filing at all.
EARNINGS_UNIVERSE = FUNDAMENTALS_UNIVERSE
EARNINGS_START = FUNDAMENTALS_START
EARNINGS_END = FUNDAMENTALS_END
