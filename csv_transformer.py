import argparse
import json
from enum import Enum
import csv

# Parse JSON data

# Entity --> Entity, if empty, use last seen
# User Story --> User Story
# Test Case ID --> Test Case ID, if empty, use last seen

# Prerequisites:
# Prerequisites, Counterparty Type, Counterparty Label, Settlement Mode, Trade Matching Status, SSI Matching Status
    # example: 
    # Prerequisites = ; Counterparty Type = ; Counterparty Label = ; Settlement Mode = ; Trade Matching Status = ; SSI Matching Status = ;
    
# Test Step:
# Test Scenario/Summary
    
# Test Data:
# Package Typology, Contract Typology
# Under <Test Data Conifigurations>:
    # Instrument, System Date, Trade Date, Value Date, Far Value Date, Fixing Date, Fix Rate, Trade Rate (Near), Trade Rate (Far), Buy CCY (Near), Buy Amt (Near), Sell CCY (Near), Sell Amt (Near),
# Buy CCY (Far), Buy Amt (Far), Sell CCY (Far), Sell Amt (Far)
    # example:
    # Package Typology = <cp> ; Contact Typology = ; Instrument = ; System Date = ; Trade Date = ; Value Date = ; Far Value Date = ; Fixing Date = ; Fix Rate = ; Trade Rate (Near) = ; Trade Rate (Far) = ; Buy CCY (Near) = ; Buy Amt (Near) = ; Sell CCY (Near) = ; Sell Amt (Near) = ; Buy CCY (Far) = ; Buy Amt (Far) = ; Sell CCY (Far) = ; Sell Amt (Far) = ;
    
# Expected Result:
# Under <Expected Outcome>:
# OSP validation, Payment Queues, Payflow Status in OPAL, Payment Message, Confirmation (OPAL Matcher), Funding (ILMS/RTGS)
    # example:
    # OSP validation = ; Payment Queues = ; Payflow Status in OPAL = ; Payment Message = ; Confirmation (OPAL Matcher) = ; Funding (ILMS/RTGS) = ;
    
# Summary:
# Test Scenario/Summary
# concat Test Scenarios of same Test Case ID, s.t.:
# line 1 summary -> line 2 summary -> line 3 summary ->


#Output: Entity, User Story, Test Case ID, Prerequisites, Test Step, Test Data, Expected Result, JIRA US Link, JIRA US Assignee, Summary


class OUTPUT_TYPE(Enum):
    # 1: overwrite whaterver is in the dictionary
    OVERWRITE = 1
    # 2: append the current value to the existing value with a semicolon, i.e.: "a = ; b = ; c ="
    SEMICOL = 2
    # 3. will require a KEY,  and concat all values with same keys and uses arrow to separate them, i.e.: "a -> b -> c"
    SUMMARY = 3
    # 4. summary concat with ":"
    SUMMARY_CONCAT_COLON = 4
    # 5. simple concat with ":"
    CONCAT_COLON = 5

def get_OUTPUT_TYPE_from_string(string):
    try:
        return OUTPUT_TYPE[string.upper()]
    except KeyError:
        raise ValueError(f"Invalid color: {string}")


# output_type = get_OUTPUT_TYPE_from_string("SEMICOL")
# output_type_number = output_type.value
# print(output_type_number)  # Output: 2

class Rule:
    def __init__(self, output_col: str, input_cols: list, output_type: OUTPUT_TYPE, key: str = None, output_last_seen: bool = False):
        self.output_col = output_col
        self.input_cols = input_cols
        self.output_type = output_type
        self.key = key
        self.output_last_seen = output_last_seen
        self.last_seen_value = ""
        self.summary_history = ""
        self.summary_key_used = ""
        
    def apply_rule(self, input: dict):
        '''
        input: given a line of csv as dict
        output: result of a single element of a dict
        '''
        key = self.output_col
        output_result = ""
        
        current_processed_value = "" # for SUMMARY_CONCAT_COLON

        for input_col in self.input_cols:
            if input_col not in input:
                continue
            
            if input[input_col] != "" and self.output_last_seen == True:
                self.last_seen_value = input[input_col]
                
            if self.output_type == OUTPUT_TYPE.OVERWRITE:
                output_result = input[input_col]
                
            if self.output_type == OUTPUT_TYPE.SEMICOL:
                output_result +=  " " + input_col + " = " + input[input_col] + " ;"
                
            if self.output_type == OUTPUT_TYPE.SUMMARY:
                if self.key == None:
                    raise ValueError("Key is required for summary output type")
                
                if self.summary_key_used != input[self.key]:
                    self.summary_key_used = input[self.key]
                    self.summary_history = input[input_col]
                else:
                    self.summary_history +=  " -> " + input[input_col]

                output_result = self.summary_history
            
            if self.output_type == OUTPUT_TYPE.SUMMARY_CONCAT_COLON:
                if self.key == None:
                    raise ValueError("Key is required for summary output type")

                if current_processed_value == "":
                    current_processed_value = input[input_col]
                else:
                    current_processed_value +=  " : " + input[input_col]
            
            if self.output_type == OUTPUT_TYPE.CONCAT_COLON:
                if output_result == "":
                    output_result = input[input_col]
                else:
                    output_result +=  " : " + input[input_col]
        
        if self.output_type == OUTPUT_TYPE.SUMMARY_CONCAT_COLON:
            if self.summary_key_used != input[self.key]:
                self.summary_history = current_processed_value
                self.summary_key_used = input[self.key]
            else:
                self.summary_history +=  " -> " + current_processed_value
            
            output_result = self.summary_history

        if self.output_last_seen == True and output_result == "":
            output_result = self.last_seen_value
        
        return key, output_result
    
class CSVTransformer:
    def __init__(self, input_file, input_rule_path, output_file):
        self.input_file = input_file
        self.output_file = output_file

        # load json from file
        with open(input_rule_path, 'r') as f:
            json_data = f.read()
            self.init_rules(json_data)

    def init_rules(self, json_data: str):
        data = json.loads(json_data)
        rules = {}
               
        for data_key, value in data.items():
            # output_col = value['output_col']
            input_cols = value['Input_col']
            output_type = get_OUTPUT_TYPE_from_string(value['output_type'])
            key = value['key'] if 'key' in value else None
            output_last_seen = value['output_last_seen'] if 'output_last_seen' in value else False
            
            rule = Rule(data_key, input_cols, output_type, key, output_last_seen)
            rules.update({data_key: rule})
        
        self.rules = rules     

    def transform(self):
        
        if len(self.rules) == 0:
            print("No rules found")
            return
        with open(self.output_file, 'w', newline='') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=self.rules.keys())
            writer.writeheader()

            # open input file and start processing each line
            with open(self.input_file, 'r') as f:
                csv_datas = csv.DictReader(f)

                # for each row of csv file
                for row in csv_datas:
                    #now we have row, key, and rule, make magic here.
                    output_row = {}                
                    for key, rule in self.rules.items():
                        output_col, output_result = rule.apply_rule(row)
                        # print(output_col, output_result)
                        assert(output_col == key)
                        output_row.update({output_col: output_result})
                        # write each row
                    writer.writerow(output_row)
                    
if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    
    argparser.add_argument("--input_file", help="input file path", type=str, default="./tests/test.csv", required=False)
    argparser.add_argument("--input_rule_path", help="input rule path", type=str, default="./config/rules.json", required=False)
    argparser.add_argument("--output_file", help="output file path", type=str, default="./out/out.csv", required=False)
    
    arguments = argparser.parse_args()
    
    transformer = CSVTransformer(arguments.input_file, arguments.input_rule_path, arguments.output_file)

    transformer.transform()