from abc import ABCMeta, abstractmethod


class ICheckHealth(metaclass=ABCMeta):
    @abstractmethod
    def pass_health(self, driver, info):
        pass


class CheckHealthOne(ICheckHealth):
    def pass_health(self, driver, info):
        ...


class CheckHealthTWO(ICheckHealth):
    def pass_health(self, driver, info):
        ...


class CheckHealthThree(ICheckHealth):
    def pass_health(self, driver, info):
        ...


class CheckHealth:
    def __init__(self, health_type):
        self.health_type = health_type

    def exec_health(self, driver, info):
        self.health_type.pass_health(driver, info)
