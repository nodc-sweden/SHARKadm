

class IndexDict(dict):

    def __getitem__(self, item: int | str):
        if isinstance(item, int):
            if len(self) < item:
                return
            return list(self.values())[item]
        return super().__getitem__(item)
