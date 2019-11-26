import requests
import pandas as pd
import numpy as np


def get_points(place):
    """
    Transfers one given place to points by following rules:
    1st place = 200p
    2nd place = 190p
    3rd place = 182p
    4th place = 176p
    5th place = 172p
    6th place = 170p
    7th place = 169p
    8th place = 168p
    etc.
    """
    if type(place) != int:
        raise ValueError("Place is not an integer!")
    if place == 1:
        return 200
    if place == 2:
        return 190
    if place == 3:
        return 182
    if place == 4:
        return 176
    if place == 5:
        return 172
    if place > 175:
        return 0
    if place < 1:
        raise ValueError("Place is lower than 1!")
    else:
        return 176 - place


def clean_race_dataframe(df):
    """
    Replaces all empty strings, strings containing only whitespaces and None values by NaN.
    Replaces NaN registrations with 'nereg.'
    Drops all runners without place (DISK).
    Converts Place string to int and removes czech style of writing ordered numbers (dot at the end).
    """
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.fillna(value=np.nan, inplace=True)
    df['RegNo'] = df['RegNo'].fillna(value="nereg.")
    df.dropna(inplace=True, subset=['Place'])
    df['Place'] = df['Place'].str[:-1].astype(int)
    return df


def assign_points_to_race(df):
    for i, runner in df.iterrows():
        df.at[i, 'Points'] = get_points(runner['Place'])
    df = df.astype({'Points': 'int32'})
    return df


def export_race_to_csv(df, race_id):
    df.to_csv("points_{}.csv".format(race_id), sep=',', index=False)


def print_help():
    print("Dostupné příkazy:")
    print("\thelp\t...\tvypíše nápovědu")
    print("\trace\t...\tpřiřadí body k jednomu závodu")
    print("\toverall\t...\tvypočítá přůběžné výsledky pro všechny závody ve složce")
    print("\tquit\t...\tukončí program")


def resolve_command(command):
    if command == "help":
        print_help()
    elif command == "race":
        race_mode()
    elif command == "overall":
        print("Dosud nepodporováno.")
    elif command == "quit":
        return
    else:
        print("Neznámý příkaz: '{}'".format(command))


def race_mode():
    # Select race by it's ORIS-id
    try:
        race_id = int(input("Zadej ORIS-id závodu:\n---> "))
    except ValueError:
        print("Zadaný vstup není validní (pravděpodobně není číslo).")
        return

    # First, load name and date of selected race. Then load its results.
    url = "https://oris.orientacnisporty.cz/API/?format=json&method=getEvent&id={}".format(race_id)
    try:
        response = requests.get(url)
        data = response.json()
        if data['Status'] != 'OK':
            print("Nepodařilo se stáhnout závod z ORISu. (ORIS status: {})".format(data['Status']))
        else:
            name = data['Data']['Name']
            date = data['Data']['Date']
            print("Jméno závodu:", name)
            print("Datum závodu:", date)

            # Load results of selected race
            url = "https://oris.orientacnisporty.cz/API/?format=json&method=getEventResults&eventid={}".format(race_id)
            response = requests.get(url)
            data = response.json()
            columns_to_keep = ['ClassDesc', 'Place', 'Name', 'RegNo', 'UserID', 'Time']

            results = clean_race_dataframe(
                pd.DataFrame.from_dict(data['Data'], orient='index').set_index('ID')[columns_to_keep]
            )

            results_with_points = assign_points_to_race(results)
            export_race_to_csv(results_with_points, race_id)
            print("Závod úspěšně vyhodnocen a uložen do: points_{}.csv".format(race_id))
    except requests.exceptions.ConnectionError:
        print("Nepodařilo se připojit se k ORISU. Zkontroluj prosím své připojení k internetu.")


if __name__ == '__main__':
    print("=== BZL - výpočet průběžných výsledků ===")
    print_help()
    command = None
    while command != 'quit':
        command = input("> ")
        resolve_command(command)




