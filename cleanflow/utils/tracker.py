# track changes made to the DataFrame during cleaning operations
class ChangeTracker:
    def __init__(self):
        self.changes = []

    def log(self, message: str):
        self.changes.append(message)

    def summary(self):
        return "\n".join(self.changes)