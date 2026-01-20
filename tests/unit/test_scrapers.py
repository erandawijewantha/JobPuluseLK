import pytest
from src.scrapers.topjobs import TopJobsScraper
from src.spark.lib.text_utils import normalize_title, normalize_company, extract_district

class TestTextNormalization:
    def test_normalize_title_basic(self):
        assert normalize_title("Senior Software Engineer") == "senior software engineer"

    def test_normalize_title_abbreviations(self):
        assert normalize_title("Sr. Dev") == "senior developer"

    def test_normalize_title_urgent(self):
        assert normalize_title("Python Developer - URGENT") == "python developer"

    def test_normalize_company_pvt_ltd(self):
        assert normalize_company("ABC (Pvt) Ltd") == "abc"

    def test_normalize_company_plc(self):
        assert normalize_company("XYZ Holdings PLC") == "xyz holdings"

    def test_extract_district_colombo(self):
        assert extract_district("Colombo 03") == "Colombo"

    def test_extract_district_suburb(self):
        assert extract_district("Nugegoda") == "Colombo"

    def test_extract_district_unknown(self):
        assert extract_district("Remote") == "Unknown"