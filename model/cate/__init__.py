import numpy as np
import statsmodels.api as sm
#Metrics for the performance tracking

def power_law(x, a, b):
    """
    The power law dependency
    """
    return np.exp(b)*(x**a)

def linear_law(x, a):
    """
    Linear law dependency
    """
    return np.dot(x,a)


def stat_ols(y, X):
    """
    Stat model OLS
    """
    #X = sm.add_constant(X)
    ols =  sm.OLS(y,X)
    results = ols.fit()
    
    return results, ols

def LinearInference(df_train, features, target):
    """
    Linear fit for Log-Log and Log-Linear cases
    """
    #Fitting Log-Linear model
    
    log_results, log_ols = stat_ols(df_train[target], df_train[features])
    log_train_predict = linear_law(df_train[features], log_results.params[:])

    int_results, int_ols = stat_ols(df_train[target], df_train[features])
    abs_train_predict = linear_law(df_train[features], int_results.params[:])
        
    #Calculate the metrics MSE:
    df_train['log_mse'] = np.round(mse(df_train[target], log_train_predict),2)
    df_train['abs_mse'] = np.round(mse(df_train[target], abs_train_predict),2)
    
    #Calculate AIC
    df_train['log_aic'], df_train['abs_aic'] = np.round(log_results.aic,2), np.round(int_results.aic,2)

    #Calculate the metrics R2:
    df_train['log_r2'] = np.round(r2_score(df_train[target], log_train_predict),2)
    df_train['abs_r2'] = np.round(r2_score(df_train[target], abs_train_predict),2)

    #if log_results.f_pvalue < 0.05 and int_results.f_pvalue < 0.05:
    return df_train, log_results, int_results