"""Script for crash analysis."""
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number


def males_killed_greater_than_2(primary_person_df):
    """
    A function to return the number of males killed in an accident.
    :param primary_person_df: References the Primary_person_use.csv dataset
    :return: Returns the row count of the dataframe.
    """
    crash_df = primary_person_df.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 2))
    return crash_df.count()


def two_wheeler_count(units_df):
    """
    A function to return the number of two-wheelers involved in\
     the accident matching motorcycles or police motocycles.
    :param units_df: References the Units_use.csv dataset
    :return: Returns the count of two-wheelers.
    """
    two_wheeler_df = units_df.filter(
        (col('VEH_BODY_STYL_ID') == 'MOTORCYCLE') | (col('VEH_BODY_STYL_ID') == 'POLICE MOTORCYCLE'))
    return two_wheeler_df.count()


def top_5_car_crash(person_df, units_df):
    """
    A function to return the top 5 car models reporting deaths and air-bags not deployed.
    :param units_df: References the Units_use.csv dataset
    :param person_df: References the Primary_person_use.csv dataset
    :return: Returns the names of car models.
    """
    vehicle_df = (person_df.join(units_df, on='CRASH_ID', how='inner')
                  .filter((col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')
                          & (col('PRSN_INJRY_SEV_ID') == 'KILLED')
                          & (col('PRSN_TYPE_ID') == 'DRIVER')
                          & (col('VEH_MAKE_ID') != 'NA'))
                  .groupby('VEH_MAKE_ID').count()
                  .orderBy('count', ascending=False)
                  .limit(5))

    return vehicle_df.select(col('VEH_MAKE_ID'))


def valid_driver_license_count(primary_person_df, units_df):
    """
    A function to get the count of drivers with a valid driver's license involved in a hit-and-run.
    :param primary_person_df: References the Primary_person_use.csv dataset
    :param units_df: References the Units_use.csv dataset
    :return: Returns the count of hit-and-run drivers.
    """
    valid_license_df = (primary_person_df.join(units_df, on='CRASH_ID', how='inner')
                        .filter(((col('DRVR_LIC_TYPE_ID') == 'DRIVER LICENSE') |
                                 (col('DRVR_LIC_TYPE_ID') == 'COMMERCIAL DRIVER LIC.')) &
                                (col('VEH_HNR_FL') == 'Y'))).count()
    return valid_license_df


def state_with_highest_accidents_no_females(primary_person_df):
    """
    A function to return highest number of accidents in which females are not involved.
    :param primary_person_df: References the Primary_person_use.csv dataset
    :return: Returns a series for State with the highest count of accidents.
    """
    non_females_df = (primary_person_df.filter(col('PRSN_GNDR_ID') != 'FEMALE')
                      .groupby('DRVR_LIC_STATE_ID')
                      .count().orderBy('count', ascending=False).limit(1))
    return non_females_df.select(col('DRVR_LIC_STATE_ID'))


def vehicle_models_with_most_injurie(primary_person_df, units_df):
    """
    A function to report the top 3rd to 5th vehicle models that contribute to
     the largest number of injuries including death.
    :param primary_person_df: References the Primary_person_use.csv dataset
    :param units_df: References the Units_use.csv dataset
    :return: Returns a tuple of the 3rd to 5th vehicle model names contributing to injuries and death.
    """
    valid_license_df = (primary_person_df.join(units_df, on='CRASH_ID', how='inner')
                        .filter(((col('PRSN_INJRY_SEV_ID') != 'NOT INJURED') |
                                 (col('PRSN_INJRY_SEV_ID') != 'UNKNOWN')) &
                                (col('VEH_MAKE_ID') != 'NA'))
                        .groupby('VEH_MAKE_ID').count().orderBy('count', ascending=False)).collect()

    return valid_license_df[2].asDict()['VEH_MAKE_ID'], valid_license_df[4].asDict()['VEH_MAKE_ID']


def top_ethnic_group_body_style(primary_person_df, units_df):
    """
    A function to return the top ethnic user group of each unique body style.
    :param primary_person_df: References the Primary_person_use.csv dataset
    :param units_df: References the Units_use.csv dataset
    :return: Returns a dataframe for the top ethnic group.
    """
    primary_person_df = primary_person_df.filter(~primary_person_df.PRSN_ETHNICITY_ID.isin(['UNKNOWN', 'OTHER']))
    units_df = units_df.filter(
        (~units_df.VEH_BODY_STYL_ID.isin(['NA', 'UNKNOWN', 'OTHER  (EXPLAIN IN NARRATIVE)', 'NOT REPORTED'])))

    ethnic_group_df = primary_person_df.join(units_df, on='CRASH_ID', how='inner')
    ethnic_group_df = (
        ethnic_group_df.groupby('PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID').count().orderBy('count', asceding=False)
        .withColumn('rn',
                    row_number().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc()))).filter(
            'rn == 1'))
    return ethnic_group_df.select(col('PRSN_ETHNICITY_ID'), col('VEH_BODY_STYL_ID'))


def crash_due_to_alcohol_by_zip_code(primary_person_df):
    """
    A function to return top 5 Zip Codes with the highest number crashes with alcohols.
    :param primary_person_df:  References the Primary_person_use.csv dataset.
    :return: Returns a dataframe with zip codes.
    """
    crash_df = primary_person_df.select(col('PRSN_ALC_RSLT_ID'), col('DRVR_ZIP')).filter(col('DRVR_ZIP') != 'NULL')
    crash_df = crash_df.filter(col('PRSN_ALC_RSLT_ID') == 'Positive').groupby('DRVR_ZIP').count().orderBy('count',
                                                                                                          ascending=False).limit(
        5)
    return crash_df.select(col('DRVR_ZIP'))


def crash_id_with_no_property_damage(damages_df, units_df):
    """
    A function to return Distinct Crash IDs where No Damaged Property
    was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and
    car avails Insurance.
    :param damages_df: References the Damages_use.csv dataset
    :param units_df: References the Units_use.csv dataset
    :return: Returns a dataframe listing crash IDs.
    """
    property_damage_df = (damages_df.join(units_df, on='CRASH_ID', how='inner')
                          .filter(col('FIN_RESP_TYPE_ID')
                                  .isin(['PROOF OF LIABILITY INSURANCE', 'LIABILITY INSURANCE POLICY']))
                          .filter(~col('VEH_DMAG_SCL_1_ID').isin(['NA', 'NO DAMAGE', 'DAMAGED 1 MINIMUM', 'DAMAGED 2',
                                                                  'DAMAGED 3', 'DAMAGED 4', 'INVALID VALUE'])
                                  | (~col('VEH_DMAG_SCL_2_ID').isin(
        ['NA', 'NO DAMAGE', 'DAMAGED 1 MINIMUM', 'DAMAGED 2',
         'DAMAGED 3', 'DAMAGED 4', 'INVALID VALUE'])))
                          .filter(col('DAMAGED_PROPERTY') == 'NONE')
                          .select(col('CRASH_ID')).distinct()
                          )
    return property_damage_df.select(col('CRASH_ID')).count()


def speeding_offence(primary_person_df, units_df, charges_df):
    """
    A function to return Top 5 Vehicle Makes, charged with speeding related offences,
     has licensed Drivers, used top 10 used vehicle colours
     and has car licensed with the Top 25 states.
    :param charges_df: References the Charges_use.csv dataset
    :param primary_person_df: References the Primary_person_use.csv dataset
    :param units_df: References the Units_use.csv dataset
    :return: Returns a dataframe with top 5 vehicle models.
    """
    speeding_df = charges_df.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID')
    color_df = (units_df.groupby('VEH_COLOR_ID').count().orderBy('count', ascending=False)
                .filter(col('VEH_COLOR_ID') != 'NA').limit(10).select('VEH_COLOR_ID'))

    vehicles_top_colors_df = units_df.join(color_df, 'VEH_COLOR_ID')

    licensed_df = primary_person_df.filter(col('DRVR_LIC_TYPE_ID').isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']))

    state_offenses_df = primary_person_df.groupBy('DRVR_LIC_STATE_ID').count().orderBy('count', ascending=False).limit(25)
    top_25_states_list = state_offenses_df.select('DRVR_LIC_STATE_ID').rdd.flatMap(lambda x: x).collect()
    top_25_states = (primary_person_df.filter(col('DRVR_LIC_STATE_ID').isin(top_25_states_list))
                     .groupby('DRVR_LIC_STATE_ID').count()
                     .orderBy('count', ascending=False).limit(25))

    drivers_in_top_states_df = primary_person_df.join(top_25_states, 'DRVR_LIC_STATE_ID')

    joined_df = (speeding_df.join(licensed_df, 'CRASH_ID')
                 .join(vehicles_top_colors_df, 'CRASH_ID')
                 .join(drivers_in_top_states_df, 'CRASH_ID'))

    top_5_vehicle_models = (joined_df.groupby('VEH_MAKE_ID').count()
                           .orderBy('count', ascending=False).limit(5))

    return top_5_vehicle_models.select(col('VEH_MAKE_ID'))


