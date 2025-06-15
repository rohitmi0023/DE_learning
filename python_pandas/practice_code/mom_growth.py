# pandas
import pandas as pd

def month_over_month(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the month-over-month growth rate of claims.

    Args:
        df (pd.DataFrame): DataFrame with columns 'claim_date' and 'claim_amount'.

    Returns:
        pd.Series: Month-over-month growth rates indexed by month.
    """
    # Ensure claim_date is datetime
    df['claim_date'] = pd.to_datetime(df['claim_date'])
    # Group by year and month, sum claim_amount
    monthly = df.groupby(df['claim_date'].dt.to_period('M'))['claim_amount'].sum()
    # Calculate MoM growth rate
    mom_growth = monthly.pct_change().rename('mom_growth_rate')
    return mom_growth