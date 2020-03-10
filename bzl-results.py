import requests
import pandas as pd
import numpy as np
import os
import unidecode as udc


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
    if place == 'DISK' or place == 'MS' or int(place[:-1]) > 175:
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


def assign_points(df):
    """
    Iterates through given dataframe and assigns points to every runner based on his or her place in category.
    """
    for i, runner in df.iterrows():
        df.at[i, 'Points'] = get_points(runner['Place'])
    df = df.astype({'Points': 'int32'})
    return df


def export_race_to_csv(df, race_id):
    """
    Exports race dataframe with assigned points to a .csv file with name in format: 'points_<race_id>.csv'.
    """
    if 'Points' in df.columns:
        df.to_csv("points_{}.csv".format(race_id), sep=',', index=False)


def export_class_overall_to_csv(df, class_desc):
    """
    Exports overall results dataframe to a .csv file with name in format: 'overall_<class_desc>.csv'.
    """
    df.to_csv("overall_{}.csv".format(class_desc), sep=',')


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
            results_with_points = assign_points(results)
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


def get_overall_results():
    """
    Goes through all 'points_<id>.csv' files in current directory and creates overall results from points.
    """
    # Get filenames and ids of races with assigned points
    filenames = sorted([f for f in os.listdir("./") if os.path.isfile(os.path.join("./", f))])
    race_filenames = [f for f in filenames if f[:7] == "points_" and f[-4:] == ".csv"]
    race_ids = [int(f[7:-4]) for f in race_filenames]

    if len(race_filenames) > 0:
        races = {}
        columns_list = ['Name', 'RegNo']
        # For each race add <id>-Place and <id>-Points column
        for r_id, r_filename in zip(race_ids, race_filenames):
            races[r_id] = pd.read_csv(r_filename, index_col=False)
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
                                           '{}-Place'.format(r_id): [],
                                           '{}-Points'.format(r_id): []}
            # Iterate through runners
            for _, race_result in race.iterrows():
                reg_no = race_result['RegNo']
                class_desc = race_result['ClassDesc']
                # Registered runners
                if len(reg_no) == 7 and 64 < ord(reg_no[0]) < 91:
                    # Runner with this RegNo already has some results in this category in overall results
                    if reg_no in ovr_results[class_desc]['RegNo'].values:
                        reg_no_mask = ovr_results[class_desc]['RegNo'] == reg_no
                        ovr_results[class_desc].loc[reg_no_mask, '{}-Place'.format(r_id)] = race_result['Place']
                        ovr_results[class_desc].loc[reg_no_mask, '{}-Points'.format(r_id)] = race_result['Points']
                    # Runner with this RegNo has no results in this category in overall results so far
                    else:
                        new_runners[class_desc]['Name'].append(race_result['Name'])
                        new_runners[class_desc]['RegNo'].append(reg_no)
                        new_runners[class_desc]['{}-Place'.format(r_id)].append(race_result['Place'])
                        new_runners[class_desc]['{}-Points'.format(r_id)].append(race_result['Points'])
                # Not registered runners ('nereg.')
                else:
                    name = race_result['Name']
                    # Runner with this Name already has some results in this category in overall results
                    if name in ovr_results[class_desc]['Name'].values:
                        # Runner with this Name was already added into `new_runners`. It means two
                        # not registered runners with same name in results of one race in same class.
                        if name in new_runners[class_desc]['Name']:
                            print("POZOR: Závodník bez registračky jménem '{}' již v závodě '{}' v kategorii '{}' existuje."
                            .format(race_result['Name'], r_id, class_desc))
                        else:
                            name_mask = ovr_results[class_desc]['Name'] == name
                            ovr_results[class_desc].loc[name_mask, '{}-Place'.format(r_id)] = race_result['Place']
                            ovr_results[class_desc].loc[name_mask, '{}-Points'.format(r_id)] = race_result['Points']
                    # Runner with this Name has no results in this category in overall results so far
                    else:
                        # Runner with this Name was already added into `new_runners`. It means two
                        # not registered runners with same name in results of one race in same class.
                        if name in new_runners[class_desc]['Name']:
                            print("POZOR: Závodník bez registračky jménem '{}' již v závodě '{}' v kategorii '{}' existuje."
                            .format(race_result['Name'], r_id, class_desc))
                        else:
                            new_runners[class_desc]['Name'].append(name)
                            new_runners[class_desc]['RegNo'].append(reg_no)
                            new_runners[class_desc]['{}-Place'.format(r_id)].append(race_result['Place'])
                            new_runners[class_desc]['{}-Points'.format(r_id)].append(race_result['Points'])
            # Add all new runners to overall results of particular category
            for class_desc in ['H', 'D', 'ZV', 'HDD']:
                ovr_results[class_desc] = pd.concat(
                    [ovr_results[class_desc], pd.DataFrame.from_dict(new_runners[class_desc])],
                    ignore_index=True,
                    sort=False)
        return ovr_results
    else:
        print("Žádné závody ve složce nenalezeny.")
        return None


def solve_duplicities(input_results):
    output_results = {}
    for class_desc in ['H', 'D', 'ZV', 'HDD']:
        columns = input_results[class_desc].columns
        output_results[class_desc] = {}
        for column in columns:
            output_results[class_desc][column] = []
    # Iterate through all categories and try to merge probable duplicities
    deleted_ids = []    # list of all runners that were merged into another ones
    for class_desc in ['H', 'D', 'ZV', 'HDD']:
        for i_actual, runner in input_results[class_desc].iterrows():
            # i_actual runner wasn't merged with a previous one yet
            if i_actual not in deleted_ids:
                duplicity_solved = False
                # Iterate through all other runners that could possible be duplicates of this one
                for i_other in range(i_actual+1, input_results[class_desc].shape[0]):
                    # Lowercase names without diacritics matches => duplicity to solve
                    if udc.unidecode(runner['Name']).lower() == udc.unidecode(input_results[class_desc].loc[i_other, 'Name']).lower():
                        print(70*"=")
                        print("DUPLICITY\t|\tLeft:\t\t\t|\tRight:")
                        print(70*"-")
                        print("Name:\t\t|\t{}\t|\t{}".format(runner['Name'], input_results[class_desc].loc[i_other, 'Name']))
                        print("RegNo:\t\t|\t{}\t\t\t|\t{}".format(runner['RegNo'], input_results[class_desc].loc[i_other, 'RegNo']))
                        for descriptor in runner.index[2:]:
                            print("{}:\t|\t{}\t\t\t|\t{}".format(descriptor, runner[descriptor],
                                                                 input_results[class_desc].loc[i_other, descriptor]))
                        merge = input("Merge and keep left (l) / Merge and keep right (r) / Keep separate (s)? ")
                        while merge not in ['l', 'r', 's']:
                            merge = input("Merge and keep left (l) / Merge and keep right (r) / Keep separate (s)? ")
                        # Merge and keep left values primarily
                        if merge == 'l':
                            for descriptor in columns:
                                if runner[descriptor] is not np.nan:
                                    output_results[class_desc][descriptor].append(runner[descriptor])
                                elif input_results[class_desc].loc[i_other, descriptor] is not np.nan:
                                    output_results[class_desc][descriptor].append(input_results[class_desc].loc[i_other, descriptor])
                                else:
                                    output_results[class_desc][descriptor].append(np.nan)
                            deleted_ids.append(i_other)
                            duplicity_solved = True
                        # Merge and keep right values primarily
                        elif merge == 'r':
                            for descriptor in columns:
                                if input_results[class_desc].loc[i_other, descriptor] is not np.nan:
                                    output_results[class_desc][descriptor].append(input_results[class_desc].loc[i_other, descriptor])
                                elif runner[descriptor] is not np.nan:
                                    output_results[class_desc][descriptor].append(runner[descriptor])
                                else:
                                    output_results[class_desc][descriptor].append(np.nan)
                            deleted_ids.append(i_other)
                            duplicity_solved = True
                        # Keep both runners separately
                        elif merge == 's':
                            pass
                # This runner was not written to the output_results during duplicity solving (writing standard cases)
                if not duplicity_solved:
                    for descriptor in columns:
                        output_results[class_desc][descriptor].append(runner[descriptor])
    for class_desc in ['H', 'D', 'ZV', 'HDD']:
        output_results[class_desc] = pd.DataFrame.from_dict(output_results[class_desc])
    return output_results


def best_n_races(results):
    for class_desc in ['H', 'D', 'ZV', 'HDD']:
        num_of_all_races = len(results[class_desc].columns[2:]) // 2
        num_of_races_to_count = (num_of_all_races // 2) + 1
        total_points = []
        columns = results[class_desc].columns[2:]
        for _, runner in results[class_desc].iterrows():
            points = []
            total_points.append(0)
            for descriptor in columns:
                if 'Points' in descriptor:
                    if not np.isnan(runner[descriptor]):
                        points.append(int(runner[descriptor]))
                    else:
                        points.append(0)
            for race_points in sorted(points, reverse=True)[:num_of_races_to_count]:
                total_points[-1] += race_points
        results[class_desc]['Best{}-Points'.format(num_of_races_to_count)] = pd.Series(total_points)
        results[class_desc] = results[class_desc]\
                                    .sort_values('Best{}-Points'.format(num_of_races_to_count), ascending=False)\
                                    .reset_index(drop=True)
    return results


def overall_mode():
    ovr_results = get_overall_results()
    ovr_res_wout_dupl = solve_duplicities(ovr_results)
    final_results = best_n_races(ovr_res_wout_dupl)

    for class_desc in ['H', 'D', 'ZV', 'HDD']:
        final_results[class_desc].to_csv("overall_{}.csv".format(class_desc))


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
        # TODO: dodelat overeni duplicit u neregu (vypsat nejaky anomalie uzivateli a nabidnout mu reseni)
        # TODO: scitani bodu nejlepsich n vysledku z m zavodu

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




