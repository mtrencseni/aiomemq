# ------------------------------------------------------------
# User-tweakable settings
# ------------------------------------------------------------
BOOST_DIR       := boost
BOOST_LIB_DIR   := $(BOOST_DIR)/stage/lib

CXX             := g++
CXXFLAGS        := -w -std=c++20 -I$(BOOST_DIR)
LD_STATIC       := -Wl,-Bstatic
LD_DYNAMIC      := -Wl,-Bdynamic
LDLIBS          := -lboost_json

TARGET          := aiomemq
SRC             := aiomemq.cpp

# ------------------------------------------------------------
# Sanity checks
# ------------------------------------------------------------
ifeq ($(wildcard $(BOOST_DIR)),)
$(error Boost directory '$(BOOST_DIR)' not found. Clone, symlink or point BOOST_DIR to your local Boost tree.)
endif

# Optional: warn if Boost.JSON archive not present (non-fatal)
ifneq ($(wildcard $(BOOST_LIB_DIR)/libboost_json*.a),)
  JSON_LIB_OK := yes
else
  $(warning ** libboost_json*.a not found in $(BOOST_LIB_DIR). Make sure you've built/staged Boost with the JSON library.)
endif

# ------------------------------------------------------------
# Build rules
# ------------------------------------------------------------
.PHONY: all clean
all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -L$(BOOST_LIB_DIR) -o $@ $< $(LD_STATIC) $(LDLIBS) $(LD_DYNAMIC)

clean:
	$(RM) $(TARGET)
