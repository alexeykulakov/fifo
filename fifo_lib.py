import pandas as pd
import json
# from io import StringIO
# from tabulate import tabulate

class FIFO:
    def __init__(self, source_path, source_fields, buy_operations, sell_operations):
        self.file_path = source_path
        self.source_fields = source_fields
        self.buy_operations = buy_operations
        self.sell_operations = sell_operations
        self.all_operations = buy_operations + sell_operations
        
    def init(self):
        
        source_fields = self.source_fields
        source_df = pd.read_excel(self.file_path)
        source_df = source_df[ (source_df[source_fields['operation_type']].isin(self.all_operations) ) ]
        self.source_df = source_df
        
        df = self.source_df.copy()
        df['id'] = df[source_fields['order_id']]
        df['type'] = df[source_fields['operation_type']]
        df['isin'] = df[source_fields['isin']]
        df['date'] = df[source_fields['oparation_date']]
        df['qty'] = abs(df[source_fields['qty']])
        df['net'] = abs(df[source_fields['net']])
        df['price'] = df['net']/df['qty']
        df['calcBalance'] = df['qty']
        df['profit'] = 0
        df['isCalculated'] = False
        df['desc'] = ''
        df = df[['id', 'type', 'isin', 'date', 'qty', 'price', 'net', 'calcBalance', 'profit', 'isCalculated', 'desc']]
        df = df.sort_values(by=['date'])
        self.calc_df = df
        return(self.calc_df)
    
    def print_source_df(self):
        return(self.source_df)
    
    def df_to_excel(self, path):
        self.result_df.to_excel(path)
        
    def calc_df(self):
        self.calc_df(path)
    
    def print_operations(self):
        for o in self.operations_log:          
            print("=============SALE============")
            print("order: ", o['order'])
            print("ISIN: ", o['ISIN'])
            print("date: ", o['date'])
            print("qty: ", o['qty'])
            print("profit: ", o['profit'])
            print("reference operations: ")
            
            for r in o['references']:
                ref_order = r['order']
                ref_type = r['type']
                ref_qty = r['ref_qty']
                ref_profit = r['ref_profit']
                print(f'  -order: {ref_order}, type: {ref_type}, qty: {ref_qty}, profit: {round(ref_profit,2)}')

    def ops_to_file(self, filepath):
        # json_format = json.dumps(s)
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(self.operations_log, f, ensure_ascii=False, indent=4)
                
    def calc(self):
        df = self.init()
        df2 = df.copy()
        operations_log = []
        
        for i in df2.iterrows():
            opId = i[1][0]
            opType = i[1][1]
            opISIN = i[1][2]
            opDate = i[1][3]
            opAmount = i[1][4]
            opPrice = i[1][5]
            opValue = i[1][6]
            calcBalance = i[1][7]
            opProfit = i[1][8]
            isCalculated = i[1][9]
            
            sell_operations = self.sell_operations
            buy_operations = self.buy_operations
            
            df2.loc[df2.id == opId, 'isCalculated'] = True
            
            if(opType in sell_operations):
                operation = {
                    'order': opId,
                    'ISIN': opISIN,
                    'date': str(opDate),
                    'qty': opAmount,
                    'net': opValue,
                    'profit': 0,
                    'references': []
                }
                
                d = df2[ (df2['date'] < opDate) & (df2['type'].isin(buy_operations) ) & (df2['isin'] == opISIN) & (df2['calcBalance'] > 0) ]
                
                for j in d.iterrows():
                    refId = j[1][0]
                    refType = j[1][1]
                    refAmount = j[1][4]
                    refPrice = j[1][5]
                    refBalance = j[1][7]
                    refProfit = j[1][8]
                    refDesc = j[1][10]

                    saleAmount = 0

                    if refBalance >= opAmount:
                        saleAmount = opAmount
                        refBalance = refBalance - opAmount
                        opAmount = 0
                    else:
                        saleAmount = refBalance
                        opAmount = opAmount - refBalance
                        refBalance = 0

                    saleValue = saleAmount * opPrice
                    buyValue = saleAmount * refPrice
                    refProfit = saleValue - buyValue
                    opProfit += refProfit
                    
                    ref_operation = {
                        'order': refId,
                        'type': refType,
                        'ref_qty': saleAmount,
                        'ref_profit': refProfit
                   }
                    
                    operation['references'].append(ref_operation)

                    df2.loc[df2.id==refId, 'calcBalance'] = refBalance
                    df2.loc[df2.id==opId, 'calcBalance'] = 0

                    df2.loc[df2.id==refId, 'profit'] += round(refProfit,2)
                    df2.loc[df2.id==opId, 'profit'] = round(opProfit,2)
                    
                    operation['profit'] = opProfit

                    # df2.loc[df2.id==opId, 'desc'] += ' | '+ str(refId) + ': ' + str(opProfit)
                    df2.loc[df2.id==opId, 'desc'] += f' | {refId}: {round(refProfit,2)}/{round(saleAmount)}'

                operations_log.append(operation)
        self.operations_log = operations_log
        
        result = df2[['id', 'profit', 'calcBalance', 'desc']]
        match_field = self.source_fields['order_id']
        result = result.rename(columns={"id": match_field, 'desc': 'desc [ref_order: profit/qty]'})
        result = pd.merge(self.source_df, result, on=match_field, how='left')
        self.result_df = result.copy()
        return(self.result_df)