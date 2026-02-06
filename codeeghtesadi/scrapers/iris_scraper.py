import datetime
import time
from tqdm import tqdm
from automation.custom_thread import CustomThread
from automation.helpers import (
    login_iris, find_obj_and_click, cleanup, init_driver
)
from automation.scrape_helpers import scrape_iris_helper
from automation.helpers import wrap_it_with_paramsv1
from .base_scraper import BaseScraper
# Note: scrape_iris in scrape.py was decorated with @wrap_it_with_paramsv1(20, 180, True, True, True, False)

class IrisScraper(BaseScraper):
    
    @wrap_it_with_paramsv1(20, 180, True, True, True, False)
    def scrape_iris(self,
                    path=None,
                    df=None,
                    return_df=False,
                    del_prev_files=True,
                    headless=True,
                    time_out=130,
                    role='manager_phase2',
                    end_time=22,
                    shenase_dadrasi='no',
                    table_name='tbltempheiat',
                    info={}):
        self.info = info
        self.path = path

        if (role == 'manager_phase2'):
            df_final = df[0].loc[(df[0]['shenase_dadrasi_no'].str.len() < 4) & (
                df[0]['msg'].str.len() < 4)
                & (df[0]['second_phase'] == 'yes')]
            # df_final = df[0]

        elif (role == 'manager_phase1'):
            df_final = df[0].loc[(df[0]['is_done'] != 'yes')]
            # df_final = df[0].loc[(df[0]['shenase_dadrasi_no'].str.len() < 6)
            #                      & (df[0]['is_done'] != 'yes')]
            # & (
            #     df[0]['is_done'] != 'no')]
            # df_final = df[0]
        else:
            # df_final = df[0].loc[(df[0]['shenase_dadrasi_no'].str.len() < 4) & (
            #     df[0]['msg'].str.len() < 4)]
            df_final = df[0]
            # df_final = df[0].loc[(df[0]['is_done'] == 'yes')
            #                      & (df[0]['second_phase'] != 'yes')]
            # df_final = df[0]

        if not df_final.empty:
            init = True
            with self.init_driver(path=path, headless=headless) as self.driver: # Using base init_driver

                # We need to set prefs/options if they differ from base.
                # Base init_driver uses self.driver_type.
                # In original code: prefs={'maximize': True, 'zoom': '0.73'}, disable_popups=True
                # So we should probably use automation.helpers.init_driver directly to pass these args, 
                # OR update self.driver after init (but init_driver context manager does setup).
                # To be safe and identical to original, I'll use init_driver from helpers.
                pass
            
            # Re-doing the context manager using helpers.init_driver directly for custom params
            with init_driver(pathsave=path, driver_type=self.driver_type,
                             headless=headless, prefs={'maximize': True,
                                                       'zoom': '0.73'}, disable_popups=True, info=self.info,
                             ) as self.driver:

                for index, item in tqdm(df_final.iterrows()):
                    if init:
                        self.driver, self.info = login_iris(self.driver, creds={'username': str(df[1][role].iloc[0]),
                                                                                'pass': str(df[1]['pass'].iloc[0])}, info=self.info)
                        if (not self.info['success']):
                            raise Exception

                        self.driver, self.info = find_obj_and_click(
                            driver=self.driver, info=self.info, elm='OBJ', linktext='موارد اعتراض/ شکایت')
                        init = False

                    end_it = False
                    if end_it:

                        cur_time = datetime.datetime.now().hour
                        if cur_time == end_time:
                            self.driver, self.info = cleanup(
                                driver=self.driver, info=self.info)
                            return self.driver
                    stop_threads = False

                    t = CustomThread(target=scrape_iris_helper, args=(
                        lambda: stop_threads, index, item, self.driver, df,
                        role, shenase_dadrasi, table_name))

                    t.start()
                    res = t.join(time_out)
                    if ((res is None)):
                        self.driver, self.info = cleanup(
                            driver=self.driver, info=self.info, close_driver=False)
                        raise Exception
                    if (not res[1]['success']):
                        try:
                            init = True
                            self.driver, self.info = cleanup(
                                driver=self.driver, info=self.info, close_driver=False)
                        except:
                            continue

        return self.driver, self.info
