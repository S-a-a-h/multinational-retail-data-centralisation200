#I would have another class called DataTransformer or DataTransform something like that
#Then I would create all the methods that clean data inside that class
#I would then import that class into the script that has DataExtractor inside it
#When creating the clean_user_data method I would then have something like this

#def clean_user_data():
#   DataTransform.convert_col_dtype(column="Name", type="str")
#  DataTransform.clean_weights(column_name="weights")

#Then you're clean_user_data is going to look super readable and clear in what it's trying to achieve
#So a lot of what's wrong in one table might be the same thing in another table
#Also don't worry about the types too much here as milestone 3 asks you to change the types

class DataTransformer:
    