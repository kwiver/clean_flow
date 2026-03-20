# track changes made to the DataFrame during cleaning operations
class ChangeTracker:
    def __init__(self):
        self.column_changes = {}
        self.global_changes = {
            "duplicates_removed": 0
        }

    def log(self, column, change_type, count=0):
        if column not in self.column_changes:
            self.column_changes[column] = {}

        if change_type not in self.column_changes[column]:
            self.column_changes[column][change_type] = 0

        self.column_changes[column][change_type] += count

    def log_global(self, change_type, count=0):
        if change_type not in self.global_changes:
            self.global_changes[change_type] = 0

        self.global_changes[change_type] += count


    def summary(self):
        print("\n" + "=" * 40)
        print("🧼 CLEANFLOW SUMMARY")
        print("=" * 40)

        if not self.column_changes and not any(self.global_changes.values()):
            print("No changes were made.")
            print("=" * 40)
            return

        # column-level changes
        for col, changes in self.column_changes.items():
            print(f"\nColumn: {col}")
            for change, count in changes.items():
                print(f" - {change}: {count}")

        # global changes
        if self.global_changes.get("duplicates_removed", 0) > 0:
            print(f"\nDuplicates removed: {self.global_changes['duplicates_removed']}")

        print("=" * 40)