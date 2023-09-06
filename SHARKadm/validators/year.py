from .base import Validator, DataHolderProtocol


class ValidateYearNrDigits(Validator):

    def validate(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['year'] = data_holder.data['year'].apply(self.check)

    @staticmethod
    def check(x):
        if len(x) != 4:
            print('Year is not of length 4')
