from .base import Transformer, DataHolderProtocol
from SHARKadm.data.archive import ArchiveBase


class AddStatus(Transformer):
    physical_chemical_keys = [
        'PhysicalChemical'.lower(),
        'Physical and Chemical'.lower()
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds status columns'

    def _transform(self, data_holder: ArchiveBase) -> None:
        checked_by = data_holder.delivery_note['data kontrollerad av']
        if data_holder.data_type.lower() in self.physical_chemical_keys:
            data_holder.data['check_status_sv'] = 'Klar'
            data_holder.data['check_status_en'] = 'Completed'
            data_holder.data['data_checked_by_sv'] = 'Leverantor'
            data_holder.data['data_checked_by_en'] = 'Deliverer'
            # if checked_by == r'Leverantör':
            #     data_holder.data['data_checked_by_sv'] = 'Leverantör'
            #     data_holder.data['data_checked_by_en'] = 'Deliverer'
            #     data_holder.data['check_status_sv'] = 'Pågående-SMHI'
            #     data_holder.data['check_status_en'] = 'Pending-SMHI'
            # elif checked_by == r'Leverantör och Datavärd':
            #     data_holder.data['data_checked_by_sv'] = 'Leverantör och Datavärd'
            #     data_holder.data['data_checked_by_en'] = 'Deliverer and Datacenter'
            #     data_holder.data['check_status_sv'] = 'Klar'
            #     data_holder.data['check_status_en'] = 'Completed'
        else:
            data_holder.data['check_status_sv'] = 'Klar'
            data_holder.data['check_status_en'] = 'Completed'
            data_holder.data['data_checked_by_sv'] = 'Leverantor'
            data_holder.data['data_checked_by_en'] = 'Deliverer'

        print(f'{data_holder.delivery_note.status=}')

