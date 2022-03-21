import pandas as pd
import mlflow

def check_null_values(df):
    """_summary_

    Args:
        df (_type_): _description_
    """
    tab_info = pd.DataFrame(df.dtypes).T.rename(index={0:'column type'})
    tab_info = tab_info.append(pd.DataFrame(df.isnull().sum()).T.rename(index={0:'null values (nb)'}))
    tab_info = tab_info.append(pd.DataFrame(df.isnull().sum()/df.shape[0]*100).T.
                            rename(index={0:'null values (%)'}))
    print ('-' * 10 + " Display information about column types and number of null values " + '-' * 10 )
    print(tab_info)


class Logger():
    """
    """
    def __init__(self) -> None:
        pass
    
    def set_value(self):
        """
        """
        return

    def set_experiment(self):
        """_summary_
        """
        return