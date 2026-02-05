
import zope.interface


class IObserver(zope.interface.Interface):
    x = zope.interface.Attribute("foo")

    def update(self):
        pass


class Observer:
    def __init__(self) -> None:
        self.obs = []

    def add_observer(self, obs):
        self.obs.append(obs)

    def remove_observer(self, obs):
        self.obs.pop(obs)

    def notify_observers(self, field=None, fieldname=None):
        for item in self.obs:
            item.update(field, fieldname)


class DataSource(Observer):
    def __init__(self) -> None:
        self.txt1_val = ''
        self.txt2_val = ''
        self.txt3_val = ''
        self.txt4_val = ''
        self.txt5_val = ''
        self.obs = []

    def set_txt1_val(self, value):
        self.txt1_val = value
        self.notify_observers(self.obs[0].textbox_1, '1')

    def get_txt1_val(self):
        return self.txt1_val

    def set_txt2_val(self, value):
        self.txt2_val = value
        self.notify_observers(self.obs[0].textbox_2, '2')

    def get_txt2_val(self):
        return self.txt2_val

    def set_txt3_val(self, value):
        self.txt3_val = value
        self.notify_observers(self.obs[0].textbox_3, '3')

    def get_txt3_val(self):
        return self.txt3_val

    def set_txt4_val(self, value):
        self.txt4_val = value
        self.notify_observers(self.obs[0].textbox_4, '4')

    def get_txt4_val(self):
        return self.txt4_val

    def set_txt5_val(self, value):
        self.txt5_val = value
        self.notify_observers(self.obs[0].textbox_5, '5')

    def get_txt5_val(self):
        return self.txt5_val


@zope.interface.implementer(IObserver)
class Chart:
    def __init__(self, data_source) -> None:
        self.__data_source = data_source

    def update(self):
        print('Chart got updated', self.__data_source.get_value())


@zope.interface.implementer(IObserver)
class SpreadSheet:
    def __init__(self, data_source) -> None:
        self.__data_source = data_source

    def update(self):
        print('SpreadSheet got notified', self.__data_source.get_value())


if __name__ == '__main__':
    data_source = DataSource()
    sheet1 = SpreadSheet(data_source)
    sheet2 = SpreadSheet(data_source)
    chart = Chart(data_source)

    data_source.add_observer(sheet1)
    data_source.add_observer(sheet2)
    data_source.add_observer(chart)

    data_source.set_value('1')
    data_source.set_value('2')
