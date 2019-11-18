from pyspark.sql import DataFrame


class DataProcess():
    def __init__(self):
        self.data_reader = None
        self.df_raw = None
        self.df_processed_n = None

    def read_data(self):
        self.df_raw = data_read()

        # cache df_raw for higher performance
        if isinstance(self.df_raw, DataFrame):
            self.df_raw.cache()
            self.df_raw.count()

    def process_data(self):
        df_processed_1 = data_process_1(df_raws=self.df_raw)
        df_processed_2 = data_process_2(df_processed_1=df_processed_1)
        df_processed_n_1 = data_process_3(df_processed_2=df_processed_2)
        self.df_processed_n = data_process_n(df_processed_n_1=df_processed_n_1)

    def save_data(self):
        save_data(df_processed=self.df_processed_n)

        # uncache df_raw
        if isinstance(self.df_raw, DataFrame):
            self.df_raw.unpersist()
            self.df_raw.count()


def main():
    data_process_obj = DataProcess()

    data_process_obj.read_data()
    data_process_obj.process_data()
    data_process_obj.save_data()


if __name__ == '__main__':
    main()



def data_read():
    return df_raws

def data_process_1(df_raws=df_raws):
    return df_raws

def data_process_2(df_processed_1=df_raws):
    return df_raws

def data_process_3(df_processed_2=df_raws):
    return df_raws

def data_process_n(df_processed_n_1=df_raws):
    return df_raws

def save_data(df_processed=df_processed):
    pass