"""Main file to run the crash_analysis.py script"""
from src import spark, primary_person_path, units_path, damages_path, charges_path
from src.crash_analysis import males_killed_greater_than_2, two_wheeler_count, top_5_car_crash, \
    valid_driver_license_count, state_with_highest_accidents_no_females, vehicle_models_with_most_injurie, \
    top_ethnic_group_body_style, crash_due_to_alcohol_by_zip_code, crash_id_with_no_property_damage, speeding_offence


def main():
    # Q1:
    cnt_death = males_killed_greater_than_2(primary_person_path)
    print(f'Number of males death greater than 2: {cnt_death}')

    # Q2:
    two_wheeler_crash = two_wheeler_count(units_path)
    print(f'Count of two wheelers booked for crashes: {two_wheeler_crash}')


    # Q3:
    print(f'Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy:')
    top_5_car_crash(primary_person_path, units_path).show()

    # Q4
    licence_cnt = valid_driver_license_count(primary_person_path, units_path)
    print(f'Number of Vehicles with driver having valid licences involved in hit and run: {licence_cnt}')

    # Q5
    print(f'State with the highest number of accidents in which females are not involved: ')
    state_with_highest_accidents_no_females(primary_person_path).show()

    # Q6
    most_injuries = vehicle_models_with_most_injurie(primary_person_path, units_path)
    print(f'Top 3rd to 5th VEH_MAKE_IDs that contribute to a '
          f'largest number of injuries including death: {most_injuries}')

    # Q7
    print('Top ethnic user group of each unique body style:')
    top_ethnic_group_body_style(primary_person_path, units_path).show()

    # Q8:
    print('Top 5 Zip Codes with highest number crashes with '
          'alcohols as the contributing factor to a crash:')
    crash_due_to_alcohol_by_zip_code(primary_person_path).show()

    # Q9:
    print(f'Count of Distinct Crash IDs where No Damaged Property was observed '
          f'and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance:'
          f' {crash_id_with_no_property_damage(damages_path, units_path)}')

    # Q10:
    print(f'Top 5 Vehicle Makes where drivers are charged with speeding '
          f'related offences, has licensed Drivers, used top 10 used vehicle '
          f'colours and has car licensed with the Top 25 states with highest number of offences: ')
    speeding_offence(primary_person_path, units_path, charges_path).show()

    spark.stop()


if __name__ == "__main__":
    main()
