#Create all cleaning processes in this class 
#As a lot of what's wrong in one table might be the same thing in another table
#Also don't worry about the types too much here as milestone 3 asks you to change the types

#Other tips as well categorical columns are great to target with df["column_name"].unique()
#As they don't have many values so if there are errors with their rows they'll show up in the unique list of values
#Also in a later milestone you're going to be connecting some tables together with a relationship
#If that relationship doesn't work then you know there's something wrong with the cleaning

class DataTransformer:
    