from abc import ABCMeta, abstractmethod


class Image:
    ...


class IPipelineContext(metaclass=ABCMeta):
    ...


class IPipelineStep(metaclass=ABCMeta):

    def execute(self, context: IPipelineContext):
        pass


class ImageLoadingStep(IPipelineStep):

    def execute(self, context: IPipelineContext):
        print('Load The Image from source')


class ImageFilteringStep(IPipelineStep):

    def execute(self, context: IPipelineContext):
        if context is not None:
            print('Apply filtering step')


class ImageSavingStep(IPipelineStep):

    def execute(self, context: IPipelineContext):
        if context is not None:
            print('Apply saving step')


class ImageProcessingPipeline:

    def __init__(self):

        self._steps = []

    def add_step(self, step: IPipelineStep):

        self._steps.append(step)

    def execute_pipeline(self, context: IPipelineContext):

        for step in self._steps:
            step.execute(context)


if __name__ == "__main__":
    image = Image()
    pipeline = ImageProcessingPipeline()
    pipeline.add_step(ImageLoadingStep())
    pipeline.add_step(ImageFilteringStep())
    pipeline.add_step(ImageSavingStep())
    pipeline.execute_pipeline(image)
