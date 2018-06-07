# coding=utf-8
'''
by caok
py2 + anaconda
'''
import numpy as np
import pandas as pd
import time
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from dateutil.parser import parse
import seaborn as sns
from sklearn.model_selection import train_test_split

sns.set_style('whitegrid')
import xgboost as xgb
from xgboost.sklearn import XGBClassifier
from sklearn import cross_validation, metrics
from sklearn.model_selection import GridSearchCV
from matplotlib.pylab import rcParams
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report


def current_time():
    '''
    以固定格式打印当前时间
    :return:返回当前时间的字符串
    '''
    return time.strftime('%Y-%m-%d %X', time.localtime())

def read_CSV(csvPath, DeleteId=True, SupervisedLearning=True):
    '''
    读入csv文件，如果训练无监督学习删除label = -1 的列
    :param csvPath:
    :param SupervisedLearning:
    :return:
    '''
    print ("读入数据开始：" + current_time())
    df = pd.read_csv(csvPath)
    #df = pd.read_csv(csvPath)
    if DeleteId:
        df.drop('id', 1, inplace=True)
    if SupervisedLearning:
        list = np.where(df.label == -1)  # 删除-1的行，当监督学习
        df.drop(df.index[list], inplace=True)
    else:
        pass
    print ("读入数据完成：" + current_time())
    return df

def periodOfMonth(datetime):
    '''
    :param datetime: 输入时间
    :return: 输出0,1,2 分别代表上中下旬
    '''
    day = datetime.day
    if day <= 10:
        return 0
    elif 10 < day <= 20:
        return 1
    else:
        return 2


def isFridayOrSaturday(date):
    '''
    :param date:
    :return:
    '''
    dayOfWeek = date.isoweekday()
    if (dayOfWeek == 5 | dayOfWeek == 6):
        return 1
    else:
        return 0


def isBeginOrEndOfMonth(date):
    '''
    :param date:
    :return:
    '''
    day = date.day
    if (day <= 7 | day >= 25):
        return 1
    else:
        return 0


def generateDateFeature(df):
    '''
    时间特征处理函数
    :param df:
    :return:返回处理后加上时间特征的df
    '''
    print ("处理时间特征开始：" + current_time())
    df['date'] = df['date'].apply(lambda x: parse(str(x)))
    df['dayOfWeek'] = df['date'].apply(lambda x: x.isoweekday())  # 该周的第几天
    df['periodOfMonth'] = df['date'].apply(periodOfMonth)
    df['day'] = df['date'].apply(lambda x: x.day)
    df.drop('date', 1, inplace=True)
    print ("处理时间特征完成：" + current_time())


def generate_Missing_pet(df):
    '''
    添加一个新特征，missing_pct（每一行的null）
    :param df:
    :return:
    '''
    print ("生成missing_pct完成：" + current_time())
    df['missing_pct'] = df.apply(lambda x: (len(x) - x.count()) / float(len(x)), axis=1)
    print ("生成missing_pct结束：" + current_time())


def onehot_encoding(df, col):
    '''
    onehot编码
    针对的特征主要是前20列：
    categorical_features = ["f1", "f2", "f3", "f4", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14", "f15", "f16", "f17", "f18", "f19"]
    以及其他unique instance<40的列
    sklearn的onehot 默认只处理数值型类别变量(需要先 LabelEncoder )

    :param df:
    :param col:
    :return:
    '''
    print ("oneHot编码开始：" + current_time())
    print("one hot encoding for {}".format(col))
    values = df[col].values
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(values)
    # binary encode
    onehot_encoder = OneHotEncoder(sparse=False)
    integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
    onehot_encoded = onehot_encoder.fit_transform(integer_encoded)
    cate_df = pd.DataFrame(onehot_encoded,
                           columns=["cate_{}_{}".format(col, i) for i in range(onehot_encoded.shape[-1])],
                           dtype=np.int8)
    df = df.drop(col, axis=1)
    df = pd.concat([df, cate_df], axis=1)
    del cate_df
    print ("oneHot编码结束：" + current_time())
    return df


def minMaxScaler(df):
    '''
    数据归一化，因为lable取值为1，经过了onehot编码，所以用minMaxScaler，实际使用效果非常差，关于xgboost/lightgbm究竟是否需要规约化是个问题，有说需要有说不需要
    :param df:
    :return:  minMaxScaler之后的df
    '''
    print ("数据归一化开始：" + current_time())
    scaler = MinMaxScaler(feature_range=(0, 1))
    colNames = df.columns
    scaler.fit(df)
    df = scaler.transform(df)
    df = pd.DataFrame(df, columns=colNames)
    print ("数据归一化完成：" + current_time())
    return df


def fillNA(df):
    '''
     查看每一列缺失值，缺失率>90%的列，这种属性基本不携带有用的信息，直接剔除。
                  2.5%-90%的列 如果是类别行的特征(unique instance<40)，将缺失值用-1 填充，相当于“是否缺失”当成另一种类别,再进行分析，onehot编码
                   如果是连续型的特征，用中位数填充
                  <2.5%的列直接用中位数填充
    :param df:
    :return:  需要onehot编码的list
    '''
    print ("fillna开始：" + current_time())
    uniqueInsancesList = []  # 记录每一列特征有多少unique instances
    missing_pctList = []  # 记录每一列特征的missing percent
    list = []
    index = []
    for col in df.columns:
        uniqueInsancesList.append(len(df[col].unique()))
        missing_pctList.append((len(df[col]) - df[col].count()) / float(len(df[col])))
    for i, colunmName in enumerate(df.columns.values):
        if ((uniqueInsancesList[i] <= 40) & (
                         0.025 < missing_pctList[i] < 0.9)):  # unique instance  阈值设为40是调试出来的，设为50，60，70...得到的结果与40相同
            list.append(colunmName)
            index.append(i)
        else:
            pass
    for value, i in zip(list, index):
        print ("Column {} has {} unique instances,missing percent is {}".format(value, uniqueInsancesList[i],
                                                                                missing_pctList[i]))
    desc = df.describe().T. \
        assign(missing_pct=df.apply(lambda x: (len(x) - x.count()) / float(len(x))))
    df.drop(desc[desc['missing_pct'] >= 0.9].index, 1, inplace=True)
    df[list] = df[list].fillna(-1)
    df.fillna(df.median(), inplace=True)
    print ("fillna结束：" + current_time())
    return list

def modelfit(alg, df, picName, early_stopping_rounds=50, n_importance=50):
    '''
    xgboost模型训练，并作图最重要的n个特征，返回特征list
    :param alg:
    :param df:特征工程后(onehot除去)的的df
    :param picName:   保存的文件名
    :param early_stopping_rounds:
    :param n_importance:   最重要的特征
    :return:   输出模型选择的top n 特征
    '''
    print ("modelfit开始：" + current_time())
    X_train, X_test, y_train, y_test = train_test_split(df.iloc[:, 1:], df.iloc[:, :1], test_size=0.25,
                                                        random_state=33)
    alg = alg.fit(X_train, y_train,
                  eval_set = [(X_test,y_test)],
                  eval_metric = "auc",
                  early_stopping_rounds = early_stopping_rounds
                  #verbose=True
                 )
    alg_predict = alg.predict(X_test)
    alg_predprob = alg.predict_proba(X_test)
    print (classification_report(y_test, alg_predict, target_names=['0', '1']))
    print ('atec score:', roc_e_value(y_test, alg_predprob))
    feat_imp = pd.Series(alg.booster().get_fscore()).sort_values(ascending=False)
    feature_importance_list = []
    for featName in feat_imp.head(n_importance).index:
        feature_importance_list.append(featName)
    feat_imp.head(n_importance).plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score')
    plt.savefig("../output/{}".format(picName))
    print ("feature_importance_list.png 保存在了./output/{}".format(picName))
    print ("modelfit结束：" + current_time())
    return feature_importance_list

def deleteDataFromxgBoost(feature_importance_list, train_df_raw):
    '''
    如果在最重要的50个特征中，缺失比率大于>50%,当该行数据为离异数据，直接剔除
    :param feature_importance_list:
    :param train_df_raw: 这里的df为仅经过generateDateFeature(df_raw)和generate_Missing_pet(df_raw)的df
    :return: 删除离异数据（行）后的数据
    '''
    # train_df_raw = read_CSV('atec_anti_fraud_train.csv')  # 注意，这里处理的是原始数据
    print ("deleteDataFromxgBoost开始：" + current_time())
    print('处理前的数据行数为：' + str(train_df_raw.shape[0]))
    train_df_raw['important_feature_missing_pct'] = train_df_raw[feature_importance_list].apply(
        lambda x: (len(x) - x.count()) / float(len(x)), axis=1)
    train_df_raw.drop(train_df_raw[train_df_raw.important_feature_missing_pct >= 0.5].index, axis=0, inplace=True)
    train_df_raw.drop('important_feature_missing_pct', axis=1, inplace=True)
    print('处理后的数据行数为：' + str(train_df_raw.shape[0]))
    train_df_raw.to_csv("./output/train001.csv")
    print ("deleteDataFromxgBoost结束：" + current_time())


def DivideFeatureFromXgboost(alg,df,feature_importance_list):
    '''
    xgboost模型中最重要的n个特征值，两两相除组合特征
    注意,输入的df为oneHot之前的特征
    :param df:
    :param feature_importance_list:
    :return:  画图做出中重要的组合特征， 手动筛选,  输出模型选择的top n DivideFeature特征
    '''
    print ("DivideFeatureFromXgboost开始：" + current_time())
    cols =  df.columns[1:]
    for index1, feat1 in enumerate(feature_importance_list):
        for index2, feat2 in enumerate(feature_importance_list):
            if (index1 != index2):
                df[str(feat1 + '/' + feat2)] = df[feat1].div(df[feat2])
            else:
                pass
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.fillna(df.median(), inplace=True)
    DivideFeatureFromXgboost_importance_list = modelfit(alg, df.drop(cols, 1), 'DivideFeatureFromXgboost.png')
    print(" DivideFeatureFromXgboost_importance_list:")
    print  DivideFeatureFromXgboost_importance_list
    print ("DivideFeatureFromXgboost结束：" + current_time())
    return DivideFeatureFromXgboost_importance_list

def MultiplyFeatureFromXgboost(alg,df,feature_importance_list):
    '''
    xgboost模型中最重要的50个特征值，两两相乘log组合特征
    注意,输入的df为没经过onehot的特征
    :param df:
    :param feature_importance_list:  xgboost输出的重要性top n list
    :return:  画图做出中重要的组合特征,输出模型选择的top n multiplyFeature特征
    '''
    print ("MultiplyFeatureFromXgboost开始：" + current_time())
    cols =  df.columns[1:]
    for index1, feat1 in enumerate(feature_importance_list):
        for index2, feat2 in enumerate(feature_importance_list):
            if (index1 <= index2):
                df[str(feat1 + '*' + feat2)] = np.log10(df[feat1].mul(df[feat2]))
            else:
                pass
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.fillna(df.median(), inplace=True)
    MultiplyFeatureFromXgboost_importance_list = modelfit(alg, df.drop(cols, 1), 'MultiplyFeatureFromXgboost.png')
    print(" MultiplyFeatureFromXgboost_importance_list:")
    print  MultiplyFeatureFromXgboost_importance_list
    print ("MultiplyFeatureFromXgboost结束：" + current_time())
    return MultiplyFeatureFromXgboost_importance_list

def roc_e_value(label, score):
    '''
    atec比赛评价函数
    :param label:
    :param score:
    :return:
    '''
    tpr1 = roc_value(0.01, label, score)
    tpr2 = roc_value(0.005, label, score)
    tpr3 = roc_value(0.0001, label, score)
    return 0.4 * tpr1 + 0.3 * tpr2 + 0.3 * tpr3


def roc_value(threashold, label, score):
    fpr, tpr, thr = metrics.roc_curve(label, score[:, 1], pos_label=1)
    res = tpr[(fpr <= threashold).sum() - 1]
    return res



def featureSelectFromPlot(train):
    '''
    通过分析每列特征的取值与负样本比例的关系,将一些特征离散化降维处理
    :param train:
    :return:
    '''
    train['f2'] = train['f2'].apply(lambda x: 1 if x == 2 else 0)
    train['f4'] = train['f4'].apply(lambda x: 1 if x == 2 else 0)
    train['f8'] = train['f8'].apply(lambda x: 0 if x == 0 else 1)
    train['f9'] = train['f9'].apply(lambda x: 1 if x == 2 else 0)
    train.drop('f10', 1, inplace=True)
    train['f20'] = train['f20'].apply(lambda x: 1 if x == -1 else 2 if x == 3 else 3 if x == 2 else 0)
