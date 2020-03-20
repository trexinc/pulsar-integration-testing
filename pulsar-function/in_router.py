from pulsar import Function

class RoutingFunction(Function):

    def process(self, message, context):
        customer = str(context.get_function_tenant())
        context.publish("persistent://internal/inbound/corona", message, properties={ 'customer' : customer })
