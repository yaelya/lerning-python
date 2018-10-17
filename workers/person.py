# Yael Yazdi
# -------------------------------------------person
class Person():
    import re
    class_counter = 0

    def __init__(self, firstName, lastName, year_of_birth, email, phones, address):
        self.firstName = firstName
        self.lastName = lastName
        if not firstName:
            print("Error - First Name must not be empty")
        if not lastName:
            print("Error - Last Name must not be empty")

        self.year_of_birth = year_of_birth
        if Person.isValidEmail(email):
            self.__email = email
        else:
            print("This is not a valid email address")
        self.phones = phones
        self.address = address
        self.__id = Person.get_id(self)

    def set_address(self, address):
        self.address = address

    @classmethod
    def get_id(clf):
        clf.class_counter += 1
        return clf.class_counter

    # email validtion
    @classmethod
    def isValidEmail(cls, email):
        if len(email) > 7:
            if not cls.re.match(r"^([?)[a-zA-Z0-9-.])+@([a-zA-Z0-9])+hwltd\.com", email):
                return False
            else:
                return True

    def add_number(self, number):
        self.phones.append(number)

    def remove_number(self, number):
        self.phones.remove(number)

    def getFirstName(self):
        return self.firstName

    def getLastName(self):
        return self.lastName

    def get_id(self):
        return self.class_counter

    def get_email(self):
        return self.email
    #def get_year_of_birth(self):
     #   return self.year_of_birth


# -------------------------------------------phone
class Phone:
    import re
    def __init__(self, number):
        if (Phone.validNumber(number)):
            self.number = number
        else:
            print("Error - Last Name must not be empty")
            #raise (("phone number"))

    @classmethod
    def validNumber(cls,number):
        num=cls.re.match("^\+?[0-9]([0-9]|\-)*",number)
        if not num or num.end()<len(number):
             return False
        else:
            return True
# -------------------------------------------address
class Address:
    def __init__(self, country, city):
        if not country:
            print("Error - country must not be empty")
        if not city:
            print("Error - city must not be empty")
        else:
            self.country = country
            self.city = city

    def getAddress(self):
        return "Country: (), City: (), ()".format(self.country, self.city, self._addressDetails())

    def _addressDetails(self):  # abrsract function - if i call it in function, that made the function to be abstract
        raise NotImplementedError("Abstract method not implemented.")


# -------------------------------------------streetAddress
class StreetAddress(Address):

    def __init__(self, country, city, street_name, house_number):
        super().__init__(country, city)
        self.street_name = street_name
        self.house_number = house_number

    def getAddress(self):
        return "Street name: %s, House number: %s" % (self.street_name, self.house_number)


# -------------------------------------------pobAddrees

class PobAddrees(Address):
    def __init__(self, country, city, post_office_box_number):
        super().__init__(country, city)
        self.post_office_box_number = post_office_box_number

    def getAddress(self):
        return "The Post office box number: %s" % (self.post_office_box_number)


class ErrorInData(Exception):
    def __init__(self, place):
        msg = "wrong %s input" % place
        super(ErrorInData, self).__init__(msg)

