/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

#include <unordered_map>
#include <utility>

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define GOSSIP_NB  2 // send PING to GOSSIP_NB of neighbor members
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,  // startup node -> coordinator
    JOINREP,  // coordinator -> startup node
    PING,     // gossip message
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MyMember
 * NOTE: MyMember replaces the Member by using unordered_map. 
 *       I have to do this since submit.py only allow me to change MP1Node.cpp and MP1Node.h
 *
 * DESCRIPTION: Class representing a member in the distributed system
 */
// Declaration and definition here
class MyMember : public Member{
public:
	// This member's Address
	Address addr;
	// boolean indicating if this member is up
	bool inited;
	// boolean indicating if this member is in the group
	bool inGroup;
	// boolean indicating if this member has failed
	bool bFailed;
	// number of my neighbors
	int nnb;
	// the node's own heartbeat
	long heartbeat;
	// counter: how many ping sent in the current time slot
	int pingCounter;
	// counter for ping timeout: send ping
	int timeOutCounter;
	// Membership table
	unordered_map<int, MemberListEntry> memberList;
	// falied Membership table
	unordered_map<int, MemberListEntry> failedMemberList;
	// My position in the membership table
	unordered_map<int, MemberListEntry>::iterator myPos;
	// Queue for failure detection messages
	queue<q_elt> mp1q;
	/**
	 * Constructor
	 */
	MyMember(): inited(false), inGroup(false), bFailed(false), nnb(0), heartbeat(0), pingCounter(0), timeOutCounter(0) {}
	// copy constructor
	MyMember(const MyMember &anotherMember);
	// Assignment operator overloading
	MyMember& operator =(const MyMember &anotherMember);
	virtual ~MyMember() {}
};

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	MyMember *memberNode;
	char NULLADDR[6];
      
        void sendJoinRep(Address *dst);
        void recvJoinRep(char *data, int size);
        void sendPing(Address *dst);
        void recvPing(char *data, int size);
        void sendMsg(Address *dst, enum MsgTypes type);

        Address getAddressFromId(int id);
        int getIdFromAddress(Address &addr);

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	MyMember * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(MyMember *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
