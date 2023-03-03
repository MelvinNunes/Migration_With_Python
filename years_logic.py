from datetime import datetime


def add_years(start_date, years):
    try:
        return start_date.replace(year=start_date.year + years)
    except ValueError:
        return start_date.replace(year=start_date.year + years, day=28)


# ADDING 35 YEARS TO END DATE
def add_35_years_or_not(deleted_at, created_at, updated_at):
    if deleted_at == None and updated_at is not None:
        if created_at is None:
            created_at = updated_at
        end_date_obj = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S")
        # ADDING 35 YEARS TO TIME
        end_date_added_years = add_years(end_date_obj, 35)
        # REFORMATING DATE
        end_date = datetime.strptime(
            str(end_date_added_years), "%Y-%m-%d %H:%M:%S")
    else:
        end_date = deleted_at
    return end_date
