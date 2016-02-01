include ../htx.mk

SUBDIRS= hxssup hxsmsg hxstats eservd eserv_cmd stxclient htxd hxesamp show_syscfg \
	hxestorage hxecom hxedapl hxemem64 hxetape hxehd hxesctu hxefpu64 hxecd  \
	hxecache hxerng bufdisp hxeasy hxefpscr hxefabricbus

ifeq ($(HTX_RELEASE), $(filter ${HTX_RELEASE},"htxrhel72le" "htxrhel7"))
	SUBDIRS+=hxecorsa
endif

SUBDIRS_CLEAN = $(patsubst %,%.clean,$(SUBDIRS))

.PHONY: all clean ${SUBDIRS} ${SUBDIRS_CLEAN}

all: ${SUBDIRS}
	${MKDIR} ${SHIPBIN}

${SUBDIRS}:
	@echo "making dir - "${SHIPBIN}
	${MKDIR} ${SHIPBIN}
	make -C $@

clean: ${SUBDIRS_CLEAN}

${SUBDIRS_CLEAN}:
	@make -C $(@:.clean=) clean

%.clean: %
	@make -C $< clean
