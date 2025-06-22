# https://platform.stratascratch.com/coding/10299-finding-updated-records?code_type=2

# Import your libraries
import pandas as pd

# Start writing code
ms_employee_salary.head()
df2 = ms_employee_salary.groupby('id',as_index=False)['salary'].max()
merged_df = pd.merge(ms_employee_salary,df2, on=['id', 'salary'])
merged_df