"""Main file to run the crash_analysis.py script"""
from src import spark, primary_person_path, units_path, damages_path, charges_path
from src.crash_analysis import CrashAnalysis


def main():
    ca = CrashAnalysis(primary_person_path, units_path, charges_path, damages_path)
    # Q1:
    cnt_death = ca.males_killed_greater_than_2()
    print(f'Q1: Number of males death greater than 2: {cnt_death}')

    # Q2:
    two_wheeler_crash = ca.two_wheeler_count()
    print(f'Q2: Count of two wheelers booked for crashes: {two_wheeler_crash}')

    # Q3:
    print(
        f'Q3: Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy:')
    ca.top_5_car_crash().show()

    # Q4
    licence_cnt = ca.valid_driver_license_count()
    print(f'Q4: Number of Vehicles with driver having valid licences involved in hit and run: {licence_cnt}')

    # Q5
    print(f'Q5: State with the highest number of accidents in which females are not involved: ')
    ca.state_with_highest_accidents_no_females().show()

    # Q6
    most_injuries = ca.vehicle_models_with_most_injurie()
    print(f'Q6: Top 3rd to 5th VEH_MAKE_IDs that contribute to a '
          f'largest number of injuries including death: {most_injuries}')

    # Q7
    print('Q7: Top ethnic user group of each unique body style:')
    ca.top_ethnic_group_body_style().show()

    # Q8:
    print('Q8: Top 5 Zip Codes with highest number crashes with '
          'alcohols as the contributing factor to a crash:')
    ca.crash_due_to_alcohol_by_zip_code().show()

    # Q9:
    print(f'Q9: Count of Distinct Crash IDs where No Damaged Property was observed '
          f'and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance:'
          f' {ca.crash_id_with_no_property_damage()}')

    # Q10:
    print(f'Q10: Top 5 Vehicle Makes where drivers are charged with speeding '
          f'related offences, has licensed Drivers, used top 10 used vehicle '
          f'colours and has car licensed with the Top 25 states with highest number of offences: ')
    ca.speeding_offence().show()

    spark.stop()


if __name__ == "__main__":
    main()
