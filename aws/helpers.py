def get_array_length(event, context):
  return len(event)

def test_length_equality(event, context):
  return event['length'] == event['length_post']
