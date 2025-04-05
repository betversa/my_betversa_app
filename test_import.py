# test_import.py
from modules.basketball_data import get_data
from modules.nhl_data import get_hockey_data
from modules.mlb_data import get_baseball_data

def main():
    print("Modules imported successfully!")
    # Optionally, call a simple function to verify further.
    # For example, if get_data.single exists, you might print its type or a sample output:
    # stats = get_data.single(2025, "per_game", additional_data=True)
    # print(stats.head())

if __name__ == "__main__":
    main()
