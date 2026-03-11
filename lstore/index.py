class Index:

    def add_to_index(self, column, value, rid):
        self.indices[column].insert(value, rid)

    def remove_from_index(self, column, value, rid):

        rids = self.indices[column].find(value)

        if rid in rids:
            rids.remove(rid)

    def update_index(self, column, old_value, new_value, rid):

        if old_value == new_value:
            return

        self.remove_from_index(column, new_value, rid)
        self.add_to_index(column, old_value, rid)
