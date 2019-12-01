import requests
import pandas as pd
import numpy as np
import os


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
    ...
    175th place = 1p
    """
    if place == "1.":
        return 200
    if place == "2.":
        return 190
    if place == "3.":
        return 182
    if place == "4.":
        return 176
    if place == "5.":
        return 172
    if place == "DISK" or int(place[:-1]) > 175:
        return 0
    else:
        return 176 - int(place[:-1])


def clean_race_dataframe(df):
    """
    Replaces all empty strings, strings containing only whitespaces and None values by NaN.
    Replaces empty plaes with 'DISK'.
    Replaces NaN registrations with 'nereg.'.
    Replaces empty UserIDs (ORIS) with NaN.
    """
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.fillna(value=np.nan, inplace=True)
    df['Place'] = df['Place'].fillna(value="DISK")
    df['RegNo'] = df['RegNo'].fillna(value="nereg.")
    df['UserID'] = df['UserID'].fillna(value=np.nan)
    return df


def assign_points_to_race(df):
    """
    Iterates through given dataframe and assigns points to every runner based on his or her place in category.
    """
    for i, runner in df.iterrows():
        df.at[i, 'Points'] = get_points(runner['Place'])
    df = df.astype({'Points': 'int32'})
    return df


def export_race_to_csv(df, race_id):
    """
    Exports race dataframe with assigned points to a .csv file with name in fomrat: points_<race_id>.csv.
    """
    if 'Points' in df.columns:
        df.to_csv("points_{}.csv".format(race_id), sep=',', index=False)


def race_mode(race_id):
    """
    Single race mode.
    Reads race's ORIS id from user's input, loads the race from ORIS, assigns points and exports the result into a .csv file.
    """
    # Select race by it's ORIS-id
    try:
        race_id = int(race_id)
    except ValueError:
        print("'{}' není celé číslo.".format(race_id))
        return

    # First, load name and date of selected race. Then load its results.
    url = "https://oris.orientacnisporty.cz/API/?format=json&method=getEvent&id={}".format(race_id)
    try:
        response = requests.get(url)
        data = response.json()
        if data['Status'] == 'OK':
            name = data['Data']['Name']
            date = data['Data']['Date']
            print("Jméno závodu:", name)
            print("Datum závodu:", date)

            # Load results of selected race
            url = "https://oris.orientacnisporty.cz/API/?format=json&method=getEventResults&eventid={}".format(race_id)
            response = requests.get(url)
            data = response.json()
            columns_to_keep = ['ClassDesc', 'Place', 'Name', 'RegNo', 'UserID', 'Time']

            # Clean dataset
            try:
                results = clean_race_dataframe(
                    pd.DataFrame.from_dict(data['Data'], orient='index').set_index('ID')[columns_to_keep]
                )
            except KeyError:
                print("CHYBA: Závod je ve špatném formátu (chybí mu ID výsledku). Určitě jsi zadal správné id závodu?")
                return

            # Assign points
            results_with_points = assign_points_to_race(results)
            # Export to .csv
            export_race_to_csv(results_with_points, race_id)
            print("Závod úspěšně vyhodnocen a uložen do: points_{}.csv".format(race_id))
        else:
            print("Nepodařilo se stáhnout závod z ORISu. (ORIS status: {})".format(data['Status']))
    except requests.exceptions.ConnectionError:
        print("Nepodařilo se připojit se k ORISU. Zkontroluj prosím své připojení k internetu.")


def list_races():
    """
    Lists all races with already assigned points in current folder. With their names and dates.
    """
    filenames = sorted([f for f in os.listdir("./") if os.path.isfile(os.path.join("./", f))])
    race_filenames = [f for f in filenames if f[:7] == "points_" and f[-4:] == ".csv"]
    race_ids = [int(f[7:-4]) for f in race_filenames]
    for filename, race_id in zip(race_filenames, race_ids):
        url = "https://oris.orientacnisporty.cz/API/?format=json&method=getEvent&id={}".format(race_id)
        try:
            response = requests.get(url)
            data = response.json()
            if data['Status'] == 'OK':
                name = data['Data']['Name']
                date = data['Data']['Date']
                print("'{}' - '{}' - {}".format(filename, name, date))
            else:
                print("Nepodařilo se stáhnout závod z ORISu. (ORIS status: {})".format(data['Status']))
        except requests.exceptions.ConnectionError:
            print("Nepodařilo se připojit se k ORISU. Zkontroluj prosím své připojení k internetu.")


def overall_mode():
    """
    Goes through all 'points_<id>.csv' files in current directory and creates overall results from points.
    Runner matching criteria:
        TODO: vyresit matching criteria
    """
    # Get filenames and ids of races with assigned points
    filenames = sorted([f for f in os.listdir("./") if os.path.isfile(os.path.join("./", f))])
    race_filenames = [f for f in filenames if f[:7] == "points_" and f[-4:] == ".csv"]
    race_ids = [int(f[7:-4]) for f in race_filenames]

    if len(race_filenames) > 0:
        races = {}
        columns_list = ['Name', 'RegNo']
        # For each race add <id>-Time, <id>-Place and <id>-Points column
        for r_id, r_filename in zip(race_ids, race_filenames):
            races[r_id] = pd.read_csv(r_filename, index_col=False)
            columns_list.append("{}-Time".format(r_id))
            columns_list.append("{}-Place".format(r_id))
            columns_list.append("{}-Points".format(r_id))
        # Create overall results - dataframe for every category
        ovr_results = {'H': pd.DataFrame(columns=columns_list),
                       'D': pd.DataFrame(columns=columns_list),
                       'ZV': pd.DataFrame(columns=columns_list),
                       'HDD': pd.DataFrame(columns=columns_list)}
        # Iterate through races and runners and add them to overall results
        for r_id in race_ids:
            race = races[r_id]
            # Create a data structure for adding new runners (have no evidence in already processed races)
            new_runners = {}
            for class_desc in ['H', 'D', 'ZV', 'HDD']:
                new_runners[class_desc] = {'Name': [],
                                           'RegNo': [],
                                           '{}-Time'.format(r_id): [],
                                           '{}-Place'.format(r_id): [],
                                           '{}-Points'.format(r_id): []}
            # Iterate through runners
            for index, race_result in race.iterrows():
                reg_no = race_result['RegNo']
                class_desc = race_result['ClassDesc']
                # Registered runners
                if reg_no != 'nereg.':
                    # Runner with this RegNo has no results in this category in overall results so far
                    if reg_no not in ovr_results[class_desc]['RegNo'].values:
                        new_runners[class_desc]['Name'].append(race_result['Name'])
                        new_runners[class_desc]['RegNo'].append(reg_no)
                        new_runners[class_desc]['{}-Time'.format(r_id)].append(race_result['Time'])
                        new_runners[class_desc]['{}-Place'.format(r_id)].append(race_result['Place'])
                        new_runners[class_desc]['{}-Points'.format(r_id)].append(race_result['Points'])
                    # Runner with this RegNo already has some results in this category in overall results
                    else:
                        reg_no_mask = ovr_results[class_desc]['RegNo'] == reg_no
                        ovr_results[class_desc].loc[reg_no_mask, '{}-Time'.format(r_id)] = race_result['Time']
                        ovr_results[class_desc].loc[reg_no_mask, '{}-Place'.format(r_id)] = race_result['Place']
                        ovr_results[class_desc].loc[reg_no_mask, '{}-Points'.format(r_id)] = race_result['Points']
                # Not registered runners ('nereg.')
                else:
                    # TODO - tady pokracovat
                    pass
            # Add all new runners to overall results of particular category
            for class_desc in ['H', 'D', 'ZV', 'HDD']:
                ovr_results[class_desc] = pd.concat(
                    [ovr_results[class_desc], pd.DataFrame.from_dict(new_runners[class_desc])],
                    ignore_index=True,
                    sort=False)
    else:
        print("Žádné závody ve složce nenalezeny.")


def print_help():
    """
    Prints help for user interface.
    """
    print("Dostupné příkazy:")
    print("\thelp\t\t...\tvypíše nápovědu")
    print("\trace <oris-id>\t...\tpřiřadí body k vybranému závodu z orisu")
    print("\tlist\t\t...\tvypíše závody s již přiřazenými body v aktuální složce")
    print("\toverall\t\t...\tvypočítá přůběžné výsledky pro všechny závody ve složce")
    print("\tquit\t\t...\tukončí program")


def resolve_command(command):
    """
    Recoqnizes what command was used and calls an appropriate function.
    """
    if command == "help":
        print_help()
    elif command[:5] == "race ":
        race_mode(command[5:])
    elif command == "race":
        print("Nebylo zadáno ORIS-id závodu. Pro nápovědu napiš 'help'.")
    elif command == "list":
        list_races()
    elif command == "overall":
        overall_mode()
    elif command == "quit":
        return
    else:
        print("Neznámý příkaz: '{}'".format(command))


if __name__ == '__main__':
    print("=== BZL - výpočet průběžných výsledků ===")
    print_help()
    command = None
    while command != 'quit':
        command = input("> ")
        resolve_command(command)




