#Import block: Define which libraries we'll use
import random #Used to create random numbers

#Style tip: Define your full process first
#The "def" keyword creates a function
#Functions are basicaly miniature programs inside your program
#Functions can be used anywhere else in your file
def main():
    """Main function describes the overall program flow"""
    print ("Running main function") #Often useful for debugging to have functions call out themselves as running
    
    #Use a function to get a list of numbers, as long as we want
    numbers = set_numbers(10)
    print(f"Main received {len(numbers)} numbers from function")

    #Use a function to print numbers one at a time
    get_numbers(numbers)

    #Use a function to compare all numbers to a random number
    random_compare(numbers)



#We can have as many functions as we want
#Mostly used to avoid repeating code
#Can also be used for organization
def set_numbers(list_length): #By providing "list_length" to function, we can easily change the specifics, without changing code
    """Creates list of list_length length, full of objects that store an assigned number"""
    print(f"Creating list of {list_length} numbers")
    numbers = [] #Create empty list to store numbers
    for i in range(list_length):
        rand = random.randrange(10) #Get random number
        new_number = NumberHolder(rand, i) #Assign number and position in list to number_holder
        numbers.append(new_number) #Add to list
    
    #Return full list
    print(f"Created list of {len(numbers)}")
    return numbers

#When to use funciton? Depends!
def get_numbers(numbers):
    print("Printing list of numbers. Numbers: ")
    for number in numbers:
        print(f"Number in position {number.return_position()}: {number.return_number()}")

def random_compare(numbers):
    print("Getting random comparison")
    compare = random.randrange(10)

    print(f"Comparing all numbers to {compare}")
    for number in numbers:
        if(number.isAbove(compare)):
            print(f"Number in position {number.return_position()} ({number.return_number()}), is above {compare}")
        else:
            print(f"Number in position {number.return_position()} ({number.return_number()}), is not above {compare}")
        


#If functions are groups of code, classes are groups of data that can execute functions
#This example is pre
class NumberHolder:
    """Number holder holds the current number"""
    def __init__(self, number, position):
        self.number = number
        self.position = position

    #Tiny annoyance: All class-defined funcitons have to take "self" as an argument, but you don't need to provide it
    def return_number(self):
        """Returns my number"""
        return self.number

    def return_position(self):
        """Returns my position"""
        return self.position

    def isAbove(self, comparison):
        """Returns true if number is above comparison"""
        return self.number>comparison
        
#All of the above was defining things. To kick it all off, we'll call "Main"
main()


