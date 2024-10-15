# Makefile for aspects of installing DiffDock

# Pip install and copy files
# May need root or bmaps-server permission
install: DEST_DIR?=/opt/diffdock
install:
	-mkdir $(DEST_DIR)
	cd $(DEST_DIR) && python3.10 -m venv venv
	rsync -az --exclude=.git --exclude=coniferpoint ../* $(DEST_DIR)/DiffDockCP
	# Make sure we fail with an error before invoking the foreach. If venv fails, the foreach will still attempt all the installs.
	cd $(DEST_DIR) && . venv/bin/activate
	# Use foreach to install each python package individually because it fails when it tries to
	# do them all together.
	cd $(DEST_DIR) && . venv/bin/activate && pip install --upgrade pip && $(foreach pkg, $(shell cat ./requirements_diffdock.txt ./requirements_service.txt), pip install $(pkg);)

install_dev: DEST_DIR?=/opt/diffdock_dev
install_dev:
	make install DEST_DIR=$(DEST_DIR)

install_qa: DEST_DIR?=/opt/diffdock_qa
install_qa:
	make install DEST_DIR=$(DEST_DIR)

# Download and cache a large torch model
seed: DIFFDOCK_DIR?=/opt/diffdock
seed:
	cd $(DIFFDOCK_DIR)/DiffDockCP && . ../venv/bin/activate && TORCH_HOME=$(DIFFDOCK_DIR)/torch python -m inference

seed_dev: DIFFDOCK_DIR?=/opt/diffdock_dev
seed_dev:
	make seed DIFFDOCK_DIR=$(DIFFDOCK_DIR)

seed_qa: DIFFDOCK_DIR?=/opt/diffdock_qa
seed_qa:
	make seed DIFFDOCK_DIR=$(DIFFDOCK_DIR)

# Install service
service:
	cp diffdock-server.service /etc/systemd/system
	systemctl daemon-reload
	service diffdock-server start

service_dev:
	cp diffdock-server_dev.service /etc/systemd/system
	systemctl daemon-reload
	service diffdock-server_dev start

service_qa:
	cp diffdock-server_qa.service /etc/systemd/system
	systemctl daemon-reload
	service diffdock-server_qa start


# Create the root directory (may need root permission)
root: DEST_DIR=/opt/diffdock
root: OWNER=bmaps-server:bmaps-server
root:
	-mkdir -p $(DEST_DIR)
	chown -R $(OWNER) $(DEST_DIR)
