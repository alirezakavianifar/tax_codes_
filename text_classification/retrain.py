import pickle
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report


def preprocess_data(df):
    df['اداره'] = df['اداره'].astype('int')
    df.dropna(subset=['آدرس'], inplace=True)

    X_train, X_test, y_train, y_test = train_test_split(
        df['آدرس'],
        df['اداره'],
        test_size=0.99,
        random_state=2022
    )

    return X_train, X_test, y_train, y_test


if __name__ == "__main__":

    path = r'E:\automating_reports_V2\automation\text_classification\رسیدگی شده مستغلات 1400.xlsx'

    df = pd.read_excel(path)

    X_train, X_test, y_train, y_test = preprocess_data(df)

    with open(r'D:\models\saved_model_ngram1_1', 'rb') as f:
        model1 = pickle.load(f)

    with open(r'D:\models\saved_model_ngram1_2', 'rb') as f:
        model2 = pickle.load(f)

    with open(r'D:\models\saved_model_ngram1_3', 'rb') as f:
        model3 = pickle.load(f)

    with open(r'D:\models\saved_model_ngram1_4', 'rb') as f:
        model4 = pickle.load(f)

    with open(r'D:\models\saved_model_ngram1_5', 'rb') as f:
        model5 = pickle.load(f)

    with open(r'D:\models\saved_model_ngram1_6', 'rb') as f:
        model6 = pickle.load(f)

    with open(r'D:\models\saved_model_ngram1_4_retrain', 'rb') as f:
        model7 = pickle.load(f)

    with open(r'E:\automating_reports_V2\saved_model_ngram1_4_retrain_', 'rb') as f:
        model8 = pickle.load(f)

    with open(r'E:\automating_reports_V2\saved_model_ngram1_4_retrain_baging', 'rb') as f:
        model9 = pickle.load(f)

    # new_model = model.fit(X_train, y_train)

    y_pred1 = model1.predict(X_test)
    y_pred2 = model2.predict(X_test)
    y_pred3 = model3.predict(X_test)
    y_pred4 = model4.predict(X_test)
    y_pred5 = model5.predict(X_test)
    y_pred6 = model6.predict(X_test)
    y_pred7 = model7.predict(X_test)
    y_pred8 = model8.predict(X_test)
    y_pred9 = model9.predict(X_test)

    print('1,1  ------' + classification_report(y_test, y_pred1))
    print('*********************************************************')
    print('*********************************************************')
    print('1,2  ------' + classification_report(y_test, y_pred2))
    print('*********************************************************')
    print('*********************************************************')
    print('1,3  ------' + classification_report(y_test, y_pred3))
    print('*********************************************************')
    print('*********************************************************')
    print('1,4  ------' + classification_report(y_test, y_pred4))
    print('*********************************************************')
    print('*********************************************************')
    print('1,5  ------' + classification_report(y_test, y_pred5))
    print('*********************************************************')
    print('*********************************************************')
    print('1,6  ------' + classification_report(y_test, y_pred6))
    print('*********************************************************')
    print('*********************************************************')
    print('1,4_retrain  ------' + classification_report(y_test, y_pred7))
    print('*********************************************************')
    print('*********************************************************')
    print('1,4_retrain_  ------' + classification_report(y_test, y_pred8))
    print('*********************************************************')
    print('*********************************************************')
    print('1,4_retrain_baging  ------' + classification_report(y_test, y_pred9))
    # with open('saved_model_new_1', 'wb') as f:
    #     pickle.dump(model, f)
