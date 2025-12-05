import pandas as pd
import numpy as np
from src.database.db import db

class CorrelationService:
    def __init__(self):
        self.db = db

    def analyze_ad_store_correlation(self, max_lag: int = 7) -> pd.DataFrame:
        """
        Calculate cross-correlation between Ad Cost and Store Sales (Square).
        Returns a DataFrame with 'Lag' (days) and 'Correlation'.
        """
        # 1. Fetch Daily Data
        query = """
        SELECT 
            m.date,
            m.ad_cost,
            r.square_sales
        FROM marts.marketing_performance m
        LEFT JOIN marts.daily_revenue r ON m.date = r.date
        WHERE m.date >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY m.date ASC
        """
        
        con = self.db.get_connection(read_only=True)
        try:
            df = con.execute(query).fetchdf()
        finally:
            con.close()
        
        if df.empty:
            return pd.DataFrame()

        # Fill NaNs
        df['ad_cost'] = df['ad_cost'].fillna(0)
        df['square_sales'] = df['square_sales'].fillna(0)
        
        # 2. Calculate Cross-Correlation
        # We want to see if Ad Cost (t) correlates with Square Sales (t + lag)
        
        correlations = []
        
        # Base correlation (Lag 0)
        # corr = df['ad_cost'].corr(df['square_sales'])
        
        for lag in range(max_lag + 1):
            # Shift sales backwards by lag days to align Sales(t+lag) with Cost(t)
            # Example: Lag 1
            # Cost(Jan 1) vs Sales(Jan 2)
            # We shift Sales column UP (negative shift in pandas means looking forward? No, shift(1) moves data down)
            # Let's use shift(-lag) on Sales? 
            # If we want Cost at T and Sales at T+1:
            # We can shift Sales column by -1.
            # Sales[T] becomes Sales[T+1].
            
            shifted_sales = df['square_sales'].shift(-lag)
            
            # Calculate correlation ignoring NaNs created by shift
            corr = df['ad_cost'].corr(shifted_sales)
            
            correlations.append({
                'Lag (Days)': lag,
                'Correlation': corr
            })
            
        return pd.DataFrame(correlations)

correlation_service = CorrelationService()
