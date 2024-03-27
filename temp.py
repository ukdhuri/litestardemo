def mask_string_for_validation(string_to_mask, reference_string, char_to_mask, char_to_replace = ' '):
    mask_locations = [i for i, char in enumerate(string_to_mask) if char == char_to_mask]
    s2_len = len(reference_string)
    reference_string = list(reference_string)
    for loc in mask_locations:
        if s2_len > loc:            
            reference_string[loc] = char_to_replace
    reference_string = ''.join(reference_string)
    return reference_string

s1 = 'aba bbb cbc'
s2 = 'xxx yy'
f=mask_string_for_validation(s1,s2,'b')
print(f)