/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
        
        MyMember *myMember = new MyMember();
        myMember->inited = member->inited;
	this->memberNode = myMember;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = 0;
	memberNode->timeOutCounter = TFAIL/2;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;

        // add itself into the member list
        MemberListEntry entry;                   
	entry.setheartbeat(memberNode->heartbeat);
	entry.settimestamp(par->getcurrtime());
        memberNode->memberList[getIdFromAddress(memberNode->addr)] = entry;
        log->logNodeAdd(&memberNode->addr, &memberNode->addr);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    memberNode->memberList.clear();
    memberNode->failedMemberList.clear();
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    if (memberNode->bFailed) {
    	return;
    }
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}


/**
 * FUNCTION NAME: sendJoinRep
 *
 * DESCRIPTION: send JOINREP msg to dst with the entire membership list
 */
void MP1Node::sendJoinRep(Address *dst)
{
    sendMsg(dst, JOINREP);
}

/**
 * FUNCTION NAME: sendPing
 *
 * DESCRIPTION: send PING msg to dst with the entire membership list
 */
void MP1Node::sendPing(Address *dst)
{
    sendMsg(dst, PING);   
}

/**
 * FUNCTION NAME: sendMsg
 *
 * DESCRIPTION: send a message to dst with the entire membership list
 *              format: msgType, no. of entries, {id, heartbeat} list
 */
void MP1Node::sendMsg(Address *dst, enum MsgTypes type)
{
    // failed node does not send message
    if (memberNode->bFailed)
        return;

    unordered_map<int, MemberListEntry>::iterator it = memberNode->memberList.begin();   
    MessageHdr *msg = NULL;
    char *head;
    size_t msgsize = sizeof(MessageHdr) + 
                     sizeof(unsigned int) +
                     memberNode->memberList.size() * (sizeof(int) + sizeof(long)) + sizeof(int) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // set msg type
    msg->msgType = type;
    head = (char *)(msg+1);

    // set no. of entries
    unsigned int listSize = memberNode->memberList.size();
    memcpy(head, &listSize, sizeof(unsigned int));
    head += sizeof(unsigned int);
    // set {id, heartbeat} membership list
    for (unsigned int i = 0; it!=memberNode->memberList.end(); ++it, ++i)
    {
        // id
        memcpy(head, 
            &it->first, sizeof(it->first));
        head += sizeof(it->first);
        // heartbeat
        memcpy(head, 
            &it->second.heartbeat, sizeof(it->second.heartbeat));
        head += sizeof(it->second.heartbeat);
    }

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, dst, (char *)msg, msgsize);

    free(msg);
}

/**
 * FUNCTION NAME: recvJoinRep
 *
 * DESCRIPTION: recv JOINREP msg store entire membership list
 */

void MP1Node::recvJoinRep(char *data, int size)
{
    if (memberNode->bFailed) {
    	return;
    }
    // join th group
    memberNode->inGroup = true;   

    // clear the list
    memberNode->memberList.clear();
    memberNode->failedMemberList.clear();

    // init the membership list    

    unsigned int listSize;
    char *head = NULL;
    head = data+sizeof(MessageHdr);
    memcpy(&listSize, head, sizeof(unsigned int));
    head = head + sizeof(unsigned int);
    // get {id, heartbeat} membership list
    for (unsigned int i = 1; i <= listSize; i++)
    {
        int id = -1;
        memcpy(&id, 
            head,
            sizeof(int));
        head = head + sizeof(int);
        long heartbeat = 0;
        memcpy(&heartbeat,
            head,
            sizeof(long));
        head = head + sizeof(long);

        int localId = getIdFromAddress(memberNode->addr); 

        MemberListEntry entry;
        // for local node, use the local heartbeat
        entry.setheartbeat(
            localId == id ? memberNode->heartbeat : heartbeat);
	entry.settimestamp(par->getcurrtime());
        memberNode->memberList.insert(pair<int,MemberListEntry>(id, entry));

        Address newNodeAddr = getAddressFromId(id);
        log->logNodeAdd(&memberNode->addr, &newNodeAddr);
    }
}

/**
 * FUNCTION NAME: recvPing
 *
 * DESCRIPTION: recv PING msg to update entire membership list
 *              Rule: 
 *                 1, if (new id) insert to actie member list
 *                 2, if (exist in active member list && new hb > local hb) 
 *                        local hb = new hb;
 *                        local timestamp = local current time
 *                 3, if (exist in faild member list)
 *                        remove from the failed member list
 *                        add to the active member list
 *                            local hb = 0;
 *                            local timestamp = local current time
 */
void MP1Node::recvPing(char *data, int size)
{
    if (memberNode->bFailed) {
    	return;
    }
    // init the membership list    

    unsigned int listSize;
    char *head = NULL;
    head = data+sizeof(MessageHdr);
    memcpy(&listSize, head, sizeof(unsigned int));
    head = head + sizeof(unsigned int);
    // get {id, heartbeat} membership list
    for (unsigned int i = 1; i <= listSize; i++)
    {
        int id = -1;
        memcpy(&id, 
            head,
            sizeof(int));
        head = head + sizeof(int);
        long heartbeat = 0;
        memcpy(&heartbeat,
            head,
            sizeof(long));
        head = head + sizeof(long);
 
        unordered_map<int, MemberListEntry>::iterator itActiveMem = memberNode->memberList.find(id);
        unordered_map<int, MemberListEntry>::iterator itFailedMem = memberNode->failedMemberList.find(id);

        if (itActiveMem != memberNode->memberList.end() &&
            itFailedMem != memberNode->failedMemberList.end())
        {
            // False Positive: a member cannot both be in active and failed member lisets.
            log->LOG(&memberNode->addr, " a member id %u cannot both be in active and failed member lisets. Stop updating list", id);
            return;
        }
 
        if (itActiveMem == memberNode->memberList.end() && 
            itFailedMem == memberNode->failedMemberList.end())
        {   // new member, insert
            MemberListEntry entry;                   
            entry.setheartbeat(heartbeat);
	    entry.settimestamp(par->getcurrtime());
            memberNode->memberList.insert(pair<int,MemberListEntry>(id, entry));

            Address newNodeAddr = getAddressFromId(id);
            log->logNodeAdd(&memberNode->addr, &newNodeAddr);
        }
        else if (itActiveMem != memberNode->memberList.end() && itActiveMem->second.getheartbeat() < heartbeat)
        {   // existing member AND receives a larger heartbeat, update
            itActiveMem->second.setheartbeat(heartbeat);
            itActiveMem->second.settimestamp(par->getcurrtime());
        }
        else if (itFailedMem != memberNode->failedMemberList.end())
        {   // receives a heartbeat from a failed node
            log->LOG(&memberNode->addr, " this is a failed id %d. do not update member list", id);
        }
    }
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 *
 * startup node --- JOINREQ --> coordinator (getjoinaddr()) // msg fmt: addr, heartbeat; ops: insert new entry in member list
 *              <-- JOINREP ---                             // msg fmt: id, num of entries, member list-(id, heartbeat) pairs 
 *              <--  PING   --> all other nodes             // msg fmt: same as JOINREP. 
                                                            // always update local entry
                                                            // update member entries for all non-local ones 
                                                                  (ops: update heartbeat, fail, remove)      
 * PING vs JOINREP: PING sends in a fixed rate. JOINREP only sent during node bootup 
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    // failed node does not send message
    if (memberNode->bFailed)
        return false;

     // 1. retrieve the message content: type
    MessageHdr msg;
    memcpy(&msg, data, sizeof(MessageHdr));

    // 2. process the message based on its type   
    switch(msg.msgType)
    {
        case JOINREQ: // format: msgType, Address, heartbeat
        {
            Member node;
            Address joinAddr = getJoinAddress();
            memcpy(&(node.addr.addr), (data+sizeof(MessageHdr)), sizeof(node.addr.addr));
            memcpy(&(node.heartbeat), (data+sizeof(MessageHdr)+sizeof(node.addr.addr)+1), sizeof(long));
            if (0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinAddr.addr), sizeof(memberNode->addr.addr)))
            {
                // insert new entry or update the existing one (recovered node) 
                // in the member list and send JOINREP to the source
                int id = getIdFromAddress(node.addr);// *(int*)(&node.addr.addr);
                MemberListEntry entry;                   
		entry.setheartbeat(node.heartbeat);
		entry.settimestamp(par->getcurrtime());
                memberNode->memberList[id] = entry;

                log->logNodeAdd(&memberNode->addr, &node.addr);

                // send JOINREP with all membership entries
                sendJoinRep(&node.addr);
            }
            else
            {
                log->LOG(&memberNode->addr, 
                    "This message is ignored since JOINREQ can only be received by the coordinator");
                return false;
            }
            break;
        }
        case JOINREP: // format msgType, no. of entries, {id, heartbeat} list
            recvJoinRep(data, size);
            break;
        case PING:
            recvPing(data, size);
            break;
        default:
            log->LOG(&memberNode->addr, "receive an unknown message (msgType=%u)", msg.msgType);
            return false;
    }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list via PING
 */
void MP1Node::nodeLoopOps() {
    if (memberNode->bFailed) {
    	return;
    }

    // update local heartbeat
    memberNode->heartbeat++;

    // update heartbeat for local node in memberlist
    unordered_map<int, MemberListEntry>::iterator it;
    it = memberNode->memberList.find(getIdFromAddress(memberNode->addr));
    if (it == memberNode->memberList.end())    
    {
        log->LOG(&memberNode->addr, "false positive: active member list doesn't have local node! Stop Process!");
        return;
    }
    it->second.heartbeat = memberNode->heartbeat;
    it->second.timestamp = par->getcurrtime();
  
    // set failed nodes
    it = memberNode->memberList.begin();
    while(it != memberNode->memberList.end())
    {
        int passed = par->getcurrtime() - it->second.timestamp;
        if (passed >= TFAIL)  // fail timer expires: mark as a failed node
        {
            log->LOG(&memberNode->addr, " id %d failed", it->first);
            memberNode->failedMemberList.insert(
                pair<int, MemberListEntry>(it->first, it->second));
            it = memberNode->memberList.erase(it); // return the next one of erased item
        }
        else
        {
            ++it;
        }
    } 

    // delete dead nodes
    it = memberNode->failedMemberList.begin();
    while (it != memberNode->failedMemberList.end())
    {
        int passed = par->getcurrtime() - it->second.timestamp;
        if (passed >= TREMOVE) // remove timer expires: delete the node from the member list
        {
            Address failedNodeAddr = getAddressFromId(it->first);
            it = memberNode->failedMemberList.erase(it);
            log->logNodeRemove(&memberNode->addr, &failedNodeAddr);         
        }
        else
        {
            ++it;
        }
    }
 
    // send PING
    if (memberNode->pingCounter >= memberNode->timeOutCounter)
    {
        if (!memberNode->memberList.empty())
        {
            // send to all other active members
           unordered_map<int, MemberListEntry>::iterator it;
            for (it = memberNode->memberList.begin();
                 it != memberNode->memberList.end(); ++it)
            {          
               // do not send PING to self; BUT we may send multiple PINGs to the same dst
               int localId = getIdFromAddress(memberNode->addr);
               if (it->first == localId)
                   continue;
            
               Address dst = getAddressFromId(it->first); 
               sendPing(&dst);
               memberNode->pingCounter = 1; // reset
            }
        }
    }   
    else
    {
        memberNode->pingCounter++;
    }
    
    return;
}

Address MP1Node::getAddressFromId(int id)
{
    Address addr;
    memset(&addr, 0, sizeof(Address));
   
	*(int *)(addr.addr) = id;
    *(short *)(&addr.addr[4]) = 0;
	return addr;
}

int MP1Node::getIdFromAddress(Address &addr)
{
    return *(int*)(&addr.addr);
}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(MyMember *memberNode) {
	memberNode->memberList.clear();
        memberNode->failedMemberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/**
 * Copy Constructor
 */
MyMember::MyMember(const MyMember &anotherMember) {
	this->addr = anotherMember.addr;
	this->inited = anotherMember.inited;
	this->inGroup = anotherMember.inGroup;
	this->bFailed = anotherMember.bFailed;
	this->nnb = anotherMember.nnb;
	this->heartbeat = anotherMember.heartbeat;
	this->pingCounter = anotherMember.pingCounter;
	this->timeOutCounter = anotherMember.timeOutCounter;
	this->memberList = anotherMember.memberList;
        this->failedMemberList = anotherMember.failedMemberList;
	this->myPos = anotherMember.myPos;
	this->mp1q = anotherMember.mp1q;
}

/**
 * Assignment operator overloading
 */
MyMember& MyMember::operator =(const MyMember& anotherMember) {
	this->addr = anotherMember.addr;
	this->inited = anotherMember.inited;
	this->inGroup = anotherMember.inGroup;
	this->bFailed = anotherMember.bFailed;
	this->nnb = anotherMember.nnb;
	this->heartbeat = anotherMember.heartbeat;
	this->pingCounter = anotherMember.pingCounter;
	this->timeOutCounter = anotherMember.timeOutCounter;
	this->memberList = anotherMember.memberList;
        this->failedMemberList = anotherMember.failedMemberList;
	this->myPos = anotherMember.myPos;
	this->mp1q = anotherMember.mp1q;
	return *this;
}
