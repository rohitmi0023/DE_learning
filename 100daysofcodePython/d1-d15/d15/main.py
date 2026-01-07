MENU = {
    "espresso": {
        "ingredients": {
            "water": 50,
            "coffee": 18,
            "milk": 0
        },
        "cost": 1.5,
    },
    "latte": {
        "ingredients": {
            "water": 200,
            "milk": 150,
            "coffee": 24,
        },
        "cost": 2.5,
    },
    "cappuccino": {
        "ingredients": {
            "water": 250,
            "milk": 100,
            "coffee": 24,
        },
        "cost": 3.0,
    }
}


# take user choice of coffee type
def user_choice():
    coffee_choice = input('What would you like? (espresso/latte/cappuccino)')
    return coffee_choice


# "off" stops the machine
def check_machine_status(coffee_choice):
    if coffee_choice == 'off':
        print('Turning off the maching!!')
        return False
    else:
        return True

# "report" choice showing all ingredients with quantity
def print_report(resources, profit):
    water = resources['water']
    milk = resources['milk']
    coffee = resources['coffee']
    print(f'Water: {water}ml')
    print(f'Milk: {milk}ml')
    print(f'Coffee: {coffee}g')
    print(f'profit: ${profit}')

def check_resources(coffee_choice, resources):
    ingredients = MENU[coffee_choice]['ingredients']
    for ingredient in ingredients:
        if resources[ingredient] < ingredients[ingredient]:
            print(f'Not enough {ingredient} to make ', coffee_choice)
            return False
        else:
            return True

# process coins when it can make coffee, quarter(0.25) dimes(0.10) nickles(0.05) pennies(0.01)
def collect_coins():
    print('Please insert coins.')
    quarter = float(input('How many quarters?: '))
    dimes = float(input('How many dimes?: '))
    nickles = float(input('How many nickles?: '))
    pennies = float(input('How many pennies?: '))
    total = 0.25*quarter + 0.10*dimes + 0.05*nickles + 0.01*pennies       
    return total
    
# check if profit inserted is sufficient. If not, refund profit. If more, then return excess.
def compare_cost(total, coffee_cost):
    total = round(total,2)
    coffee_cost = round(coffee_cost,2)
    print(f'total {total}, coffee cost {coffee_cost}')
    if total < coffee_cost:
        print('Insufficient money provided!')
    else:
        change = total - coffee_cost
        print(f'Here is {round(change, 2)} dollars in change.')
        

def play():
    resources = {
    'water': 500,
    'milk': 200,
    'coffee': 100
    }
    profit = 0
    is_machine_on = True
    while is_machine_on:
        coffee_choice = user_choice()
        is_machine_on = check_machine_status(coffee_choice)
        if not is_machine_on:
            break
        elif coffee_choice == 'report':
            print_report(resources, profit)
        elif coffee_choice in MENU:
            is_coffee_on = check_resources(coffee_choice, resources)
            if is_coffee_on:
                resources['water'] -= MENU[coffee_choice]['ingredients']['water']
                resources['coffee'] -= MENU[coffee_choice]['ingredients']['coffee'] 
                resources['milk'] -= MENU[coffee_choice]['ingredients']['milk'] 
                total = collect_coins()
                coffee_cost = MENU[coffee_choice]['cost']
                profit += coffee_cost
                compare_cost(total, coffee_cost)
                print('Enjoy your', coffee_choice)
        else:
            print('Invalid Choice!')

# print_report()
play()