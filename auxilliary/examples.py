#Define our function for finding the biggest number
def get_biggest_number():
    #We start with no biggest number
    biggest_number = 0
    #Walk through all numbers
    for number in numbers:
    #If we find a new biggest number...
        if number > biggest_number:
        #Store its value
            biggest_number=number
    
    return biggest_number

#We want to find the biggest number in this list first
numbers = [5.235, 4.2367, 7.64369, 3.565467, 4.7645434, 7.347465768, 6.345745, 5.345246,
7.3575637, 8.3246542, 5.346425, 8.346556, 8.565245245, 3.24434565, 5.465763, 7.4675]

#Use our function to get the biggest number, then print.
biggest_number=get_biggest_number()
print(f"Biggest number from list one is {biggest_number}")

#Then we want to do the same for this list
numbers = [5, 7, 2, 5, 4, 7, 6, 5, 7, 8, 5, 8, 8, 3, 5, 7, 12]

biggest_number=get_biggest_number()
print(f"Biggest number from list two is {biggest_number}")

#We start with no biggest number
biggest_number = 0
#Walk through all numbers
for number in numbers:
    #If we find a new biggest number...
    if number > biggest_number:
        #Store its value
        biggest_number=number
    
#Output biggest number
print(biggest_number)


    
#Output biggest number
print(biggest_number)


#Define our functions
def isInRange(): #Checks whether in range
    startYear = currentYear-range
    inRange = year>startYear 
    return inRange

#Check whether year is even
def isEven():
    even = year % 2 == 0
    return even

#Check whether year is on list of years to add
def isOnList():
    onList = yearsToAdd.includes(year)
    return onList

#Incoming list of years
years = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008,
        2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017,
        2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

yeasToAdd = [2003, 2006, 2010, 2012, 2014, 2015, 2016, 2017, 2020, 2021, 2024]

approvedYears10EvenOnList = [] # Starts new list for 10 years even and on list items
range = 10 #Sets range
for year in years:
    if(isInRange()):
        if(isEven()):
            if(isOnList()):
                approvedYears10EvenOnList.append(year)

approvedYears15EvenOnList = [] #Oh man I'm already tired

def checkAll(range, even, onList):
    rangeCheck = isInRange(range) #Is item within given range?
    evenCheck = isEven() == even #If "even" is true, we check whether number is even. If it's false, we check whether the number is odd
    listCheck = isOnList() == onList #Similar to above - we provide whether to make this check. 
    return rangeCheck & evenCheck & listCheck #If all are true, return true. If any are false, return false.

approvedYears10EvenOnList = [] #Stores our 10 year list with even numbers on given list
for year in years:
    if checkAll(10, True, True):
        approvedYears10EvenOnList.append(year)

approvedYears15EvenOnList = [] #As above, for 15
for year in years:
    if checkAll(15, True, True):
        approvedYears15EvenOnList.append(year)

    