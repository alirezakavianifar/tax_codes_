
import datetime
import urllib


def log(func):
    def log_it(*args, **kwargs):
        result = func(*args, **kwargs)
        if result == False:
            print('The price of coin has not changed compared to last time %s ' %
                  datetime.datetime.now())
        else:
            print('The price has changed %s ' % datetime.datetime.now())

        return result
    return log_it


def log_internet(func):
    def log_it(*args, **kwargs):
        result = func(*args, **kwargs)

        if result:
            return result
        else:
            print('The internet connection is lost. Trying again...')
            return result
    return log_it


@log_internet
def internet_on():
    try:
        request_url = urllib.request.urlopen('https://www.tgju.org/')
        return True
    except:
        return False
