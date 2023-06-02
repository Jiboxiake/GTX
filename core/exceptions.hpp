//
// Created by zhou822 on 5/22/23.
//

#ifndef BWGRAPH_V2_EXCEPTIONS_HPP
#define BWGRAPH_V2_EXCEPTIONS_HPP

#include <iostream>
#include <exception>

namespace bwgraph {
    class DeltaLockException : public std::exception {
        virtual const char *what() const throw() {
            return "Under mutual exclusion lock only current transaction should be able to modify the offset";
        }
    };

    class DeltaChainCorruptionException : public std::exception {
        virtual const char *what() const throw() {
            return "Delta Chain should not contain ";
        }
    };

    class TransactionTableOpCountException : public std::exception {
        virtual const char *what() const throw() {
            return "the operation count should never be negative";
        }
    };

    class TransactionTableMissingEntryException : public std::exception {
        virtual const char *what() const throw() {
            return "transaction entry should stay until being fully lazy updated";
        }
    };

    class DeltaChainMismatchException : public std::exception {
        virtual const char *what() const throw() {
            return "wrong delta is stored in the delta chain";
        }
    };

    class LazyUpdateException : public std::exception {
        virtual const char *what() const throw() {
            return "lazy update exception";
        }
    };
    class LabelBlockPointerException: public std::exception{
        virtual const char *what() const throw() {
            return "Label Block should not have a next pointer unless all its entries are set";
        }
    };
class BlockSafeAccessException: public std::exception{
    virtual const char *what() const throw() {
        return "the current thread should only check is_safe() iff it is accessing the block already";
    }
};
class LabelEntryMissingException : public std::exception{
    virtual const char *what() const throw() {
        return "If a transaction wrote to a label's block that label's entry should exist";
    }
};
class EdgeIteratorNoBlockToReadException : public std::exception{
    virtual const char *what() const throw() {
        return "If an edge delta block exists, there is at least one block that the current transaction can read";
    }
};
class LazyUpdateAbortException: public std::exception{
    virtual const char *what() const throw() {
        return "Lazy update will only update abort deltas during consolidation's installation phase. All other scenarios shall return ABORT";
    }
};
class GraphNullPointerException : public std::exception{
    virtual const char *what() const throw() {
        return "graph is having null pointers at a locations where it should not happen";
    }
};
class DeltaChainReclaimException:public std::exception{
    virtual const char *what() const throw() {
        return "consolidation did not capture all my in progress deltas";
    }
};
}
#endif //BWGRAPH_V2_EXCEPTIONS_HPP
