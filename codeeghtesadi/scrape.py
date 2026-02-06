from .scrapers.sanim_scraper import (
    SanimScraper, click_on_down_btn_sanim, click_on_down_btn_excelsanimforheiat,
    click_on_down_btn_excelsanimforheiatend, click_on_down_btn_excelsanim
)
from .scrapers.code_eghtesadi_scraper import CodeEghtesadiScraper
from .scrapers.iris_scraper import IrisScraper
from .scrapers.misc_scraper import MiscScraper
from .scrapers.utils import (
    list_details, select_btn_type, select_year, select_column, get_main_menu
)

class Scrape(SanimScraper, CodeEghtesadiScraper, IrisScraper, MiscScraper):
    
    def __init__(self,
                 path=None,
                 report_type=None,
                 year=None,
                 driver_type='firefox',
                 headless=True,
                 info={},
                 driver=None,
                 time_out=180,
                 lock=None,
                 table_name=None,
                 type_of=None):

        # SanimScraper (first in MRO) accepts report_type, year, table_name, type_of
        # and passes excess kwargs to next class.
        # Eventually BaseScraper accepts path, driver_type, headless, info, time_out.
        
        super().__init__(
            path=path,
            driver_type=driver_type,
            headless=headless,
            info=info,
            time_out=time_out,
            report_type=report_type,
            year=year,
            table_name=table_name,
            type_of=type_of
        )
        
        # properties not handled by Base/Sanim
        self.driver = driver
        self.lock = lock
        
        # Scrape init logic for table_name tuple
        if isinstance(self.table_name, tuple):
             self.table_name = self.table_name[0]
