

def findoptimal(seq):
    occupied = []
    notoccupied = []
    dist = {}
    for i in range(0,len(seq)):
        if int(seq[i]) == 1:
            occupied.append(i)
        else:
            notoccupied.append(i)
    if len(notoccupied) == 0:
        return 'No empty toilets ! Please wait'
    if len(occupied) == 0:
        return 'All toilets are empty! Select anyone'
    for each in notoccupied:
        val = min([abs(x - each) for x in occupied])
        dist[each]=val
    return (max(dist, key=dist.get)+1)



if __name__ == '__main__':
    seq = list(input("Enter the sequence (eg: 00100,11110) : ") )
    #seq = [0,0,1,0]
    optimal = findoptimal(seq)
    print(optimal)
